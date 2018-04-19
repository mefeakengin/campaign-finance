#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import csv
import tempfile
import time
import logging
import optparse
import locale
import sys

import dj_database_url
import psycopg2
import psycopg2.extras

import dedupe

import ConfigParser

main_dir = os.getcwd()

# Connect to the database
def connect():
    # Read configurations from file
    config = ConfigParser.ConfigParser()
    # config.read(main_dir + "/../database/dbconfig.cnf")
    config.read('/home/efe/campaign-finance/database/dbconfig.cnf')
    print config
    database = config.get('client', 'database')
    user = config.get('client', 'user')
    password = config.get('client', 'password')
    hostname = config.get('client', 'hostname')
    port = config.get('client', 'port')
    conn = psycopg2.connect(database=database, user = user, password = password, host = hostname, port = port)
    print "Opened database " + database
    return conn

# ## Setup
settings_file = 'individual_1990_settings'
training_file = 'individual_1990_training.json'

start_time = time.time()

conn = connect()
c = conn.cursor()

# Create extension array
c.execute("CREATE EXTENSION intarray")

# We'll be using variations on this following select statement to pull
# in campaign donor info.
#
# We did a fair amount of preprocessing of the fields in
# `pgsql_big_dedupe_example_init_db.py`

DONOR_SELECT = "SELECT individual_id, first_name, last_name, zip from individual_1990"

# ## Training

if os.path.exists(settings_file):
    print('reading from ', settings_file)
    with open(settings_file, 'rb') as sf:
        deduper = dedupe.StaticDedupe(sf, num_cores=4)
else:

    # Define the fields dedupe will pay attention to
    #
    # The address, city, and zip fields are often missing, so we'll
    # tell dedupe that, and we'll learn a model that take that into
    # account
    fields = [{'field': 'first_name', 'variable name': 'name',
               'type': 'String'},
              {'field': 'last_name', 'variable name': 'name',
               'type': 'String'},
              {'field': 'zip', 'variable name': 'name',
               'type': 'String'}
              ]

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields, num_cores=4)

    # Named cursor runs server side with psycopg2
    cur = con.cursor('donor_select')

    cur.execute(DONOR_SELECT)
    temp_d = dict((i, row) for i, row in enumerate(cur))

    deduper.sample(temp_d, 75000)
    del temp_d

    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    #
    # __Note:__ if you want to train from
    # scratch, delete the training_file
    if os.path.exists(training_file):
        print('reading labeled examples from ', training_file)
        with open(training_file) as tf:
            deduper.readTraining(tf)

    # ## Active learning

    print('starting active labeling...')
    # Starts the training loop. Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.

    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    dedupe.convenience.consoleLabel(deduper)
    # When finished, save our labeled, training pairs to disk
    with open(training_file, 'w') as tf:
        deduper.writeTraining(tf)

    # Notice our argument here
    #
    # `recall` is the proportion of true dupes pairs that the learned
    # rules must cover. You may want to reduce this if your are making
    # too many blocks and too many comparisons.
    deduper.train(recall=0.90)

    with open(settings_file, 'wb') as sf:
        deduper.writeSettings(sf)

    # We can now remove some of the memory hobbing objects we used
    # for training
    deduper.cleanupTraining()

## Blocking
print('blocking...')

# To run blocking on such a large set of data, we create a separate table
# that contains blocking keys and record ids
print('creating blocking_map database')
c.execute("DROP TABLE IF EXISTS blocking_map")
c.execute("CREATE TABLE blocking_map "
          "(block_key VARCHAR(200), individual_id INTEGER)")


# If dedupe learned a Index Predicate, we have to take a pass
# through the data and create indices.
print('creating inverted index')

for field in deduper.blocker.index_fields:
    c2 = con.cursor('c2')
    c2.execute("SELECT DISTINCT %s FROM individual_2002" % field)
    field_data = (row[field] for row in c2)
    deduper.blocker.index(field_data, field)
    c2.close()

# Now we are ready to write our blocking map table by creating a
# generator that yields unique `(block_key, individual_id)` tuples.
print('writing blocking map')

c3 = con.cursor('donor_select2')
c3.execute(DONOR_SELECT)
full_data = ((row['individual_id'], row) for row in c3)
b_data = deduper.blocker(full_data)

# Write out blocking map to CSV so we can quickly load in with
# Postgres COPY
csv_file = tempfile.NamedTemporaryFile(prefix='blocks_', delete=False, mode='w')
csv_writer = csv.writer(csv_file)
csv_writer.writerows(b_data)
c3.close()
csv_file.close()

f = open(csv_file.name, 'r')
c.copy_expert("COPY blocking_map FROM STDIN CSV", f)
f.close()

os.remove(csv_file.name)

con.commit()

# free up memory by removing indices
deduper.blocker.resetIndices()

# Remove blocks that contain only one record, sort by block key and
# donor, key and index blocking map.
#
# These steps, particularly the sorting will let us quickly create
# blocks of data for comparison
print('prepare blocking table. this will probably take a while ...')

logging.info("indexing block_key")
c.execute("CREATE INDEX blocking_map_key_idx ON blocking_map (block_key)")

c.execute("DROP TABLE IF EXISTS plural_key")
c.execute("DROP TABLE IF EXISTS plural_block")
c.execute("DROP TABLE IF EXISTS covered_blocks")
c.execute("DROP TABLE IF EXISTS smaller_coverage")

# Many block_keys will only form blocks that contain a single
# record. Since there are no comparisons possible withing such a
# singleton block we can ignore them.
logging.info("calculating plural_key")
c.execute("CREATE TABLE plural_key "
          "(block_key VARCHAR(200), "
          " block_id SERIAL PRIMARY KEY)")

c.execute("INSERT INTO plural_key (block_key) "
          "SELECT block_key FROM blocking_map "
          "GROUP BY block_key HAVING COUNT(*) > 1")

logging.info("creating block_key index")
c.execute("CREATE UNIQUE INDEX block_key_idx ON plural_key (block_key)")

logging.info("calculating plural_block")
c.execute("CREATE TABLE plural_block "
          "AS (SELECT block_id, individual_id "
          " FROM blocking_map INNER JOIN plural_key "
          " USING (block_key))")

logging.info("adding individual_id index and sorting index")
c.execute("CREATE INDEX plural_block_individual_id_idx ON plural_block (individual_id)")
c.execute("CREATE UNIQUE INDEX plural_block_block_id_individual_id_uniq "
          " ON plural_block (block_id, individual_id)")


# To use Kolb, et.al's Redundant Free Comparison scheme, we need to
# keep track of all the block_ids that are associated with a
# particular donor records.

logging.info("creating covered_blocks")
c.execute("CREATE TABLE covered_blocks "
          "AS (SELECT individual_id, "
          " array_agg(block_id ORDER BY block_id) "
          "   AS sorted_ids "
          " FROM plural_block "
          " GROUP BY individual_id)")

c.execute("CREATE UNIQUE INDEX covered_blocks_individual_id_idx "
          "ON covered_blocks (individual_id)")

con.commit()

# In particular, for every block of records, we need to keep
# track of a donor records's associated block_ids that are SMALLER than
# the current block's id.
logging.info("creating smaller_coverage")
c.execute("CREATE TABLE smaller_coverage "
          "AS (SELECT individual_id, block_id, "
          " sorted_ids[(idx(sorted_ids, block_id) - 1)]"
          "      AS smaller_ids "
          " FROM plural_block INNER JOIN covered_blocks "
          " USING (individual_id))")

con.commit()


## Clustering

def candidates_gen(result_set):
    lset = set

    block_id = None
    records = []
    i = 0
    for row in result_set:
        if row['block_id'] != block_id:
            if records:
                yield records

            block_id = row['block_id']
            records = []
            i += 1

            if i % 10000 == 0:
                print(i, "blocks")
                print(time.time() - start_time, "seconds")

        smaller_ids = row['smaller_ids']

        if smaller_ids:
            print "smaller_ids ", smaller_ids
            print "lset ", lset
            smaller_ids = lset([smaller_ids])
        else:
            smaller_ids = lset([])

        records.append((row['individual_id'], row, smaller_ids))

    if records:
        yield records

c4 = con.cursor('c4') # TODO: Not sure if this would be correct
c4.execute("SELECT individual_id, first_name, last_name, zip, block_id, smaller_ids "
           "FROM smaller_coverage "
           "INNER JOIN individual_2002 "
           "USING (individual_id) "
           "ORDER BY (block_id)")

print('clustering...')
clustered_dupes = deduper.matchBlocks(candidates_gen(c4),
                                      threshold=0.5)

# matchBlocks returns a generator. Turn it into a list
clustered_dupes = list(clustered_dupes)

## Writing out results

# We now have a sequence of tuples of donor ids that dedupe believes
# all refer to the same entity. We write this out onto an entity map
# table
c.execute("DROP TABLE IF EXISTS entity_map")

print('creating entity_map database')
c.execute("CREATE TABLE entity_map "
          "(individual_id INTEGER, canon_id INTEGER, "
          " cluster_score FLOAT, PRIMARY KEY(individual_id))")

csv_file = tempfile.NamedTemporaryFile(prefix='entity_map_', delete=False,
                                       mode='w')
csv_writer = csv.writer(csv_file)


for cluster, scores in clustered_dupes:
    cluster_id = cluster[0]
    for individual_id, score in zip(cluster, scores) :
        csv_writer.writerow([individual_id, cluster_id, score])

c4.close()
csv_file.close()

f = open(csv_file.name, 'r')
c.copy_expert("COPY entity_map FROM STDIN CSV", f)
f.close()

os.remove(csv_file.name)

con.commit()

c.execute("CREATE INDEX head_index ON entity_map (canon_id)")
con.commit()

# Print out the number of duplicates found
print('# duplicate sets')
print(len(clustered_dupes))


# ## Payoff

# With all this done, we can now begin to ask interesting questions
# of the data
#
# For example, let's see who the top 10 donors are.

# locale.setlocale(locale.LC_ALL, '')  # for pretty printing numbers
#
# # Create a temporary table so each group and unmatched record has a unique id
# c.execute("CREATE TEMPORARY TABLE e_map "
#           "AS SELECT COALESCE(canon_id, individual_id) AS canon_id, individual_id "
#           "FROM entity_map "
#           "RIGHT JOIN donors USING(individual_id)")
#
#
# c.execute(
#     "SELECT CONCAT_WS(' ', donors.first_name, donors.last_name) AS name, "
#     "donation_totals.totals AS totals "
#     "FROM donors INNER JOIN "
#     "(SELECT canon_id, SUM(CAST(amount AS FLOAT)) AS totals "
#     " FROM individual_2002 INNER JOIN e_map "
#     " USING (individual_id) "
#     " GROUP BY (canon_id) "
#     " ORDER BY totals "
#     " DESC LIMIT 10) "
#     "AS donation_totals ON donors.individual_id=donation_totals.canon_id "
#     "WHERE donors.donor_id = donation_totals.canon_id"
# )
#
#
# print("Top Donors (deduped)")
# for row in c.fetchall():
#     row['totals'] = locale.currency(row['totals'], grouping=True)
#     print('%(totals)20s: %(name)s' % row)
#
# # Compare this to what we would have gotten if we hadn't done any
# # deduplication
# c.execute(
#     "SELECT CONCAT_WS(' ', donors.first_name, donors.last_name) as name, "
#     "SUM(CAST(contributions.amount AS FLOAT)) AS totals "
#     "FROM donors INNER JOIN contributions "
#     "USING (donor_id) "
#     "GROUP BY (donor_id) "
#     "ORDER BY totals DESC "
#     "LIMIT 10"
# )
#
# print("Top Donors (raw)")
# for row in c.fetchall():
#     row['totals'] = locale.currency(row['totals'], grouping=True)
#     print('%(totals)20s: %(name)s' % row)


# Close our database connection
c.close()
con.close()

print('ran in', time.time() - start_time, 'seconds')
