import ConfigParser
import csv
import psycopg2
import os
import sys
import re

main_dir = os.getcwd()
# data_dir = main_dir + '/data/fec/'
data_dir = '../data/fec/'
# csv.field_size_limit(sys.maxsize)

# Read files returns the rows in the txt in a processed way
def read_txt_files(file_paths, delimiter='|'):
    lines = []
    for path in file_paths:
        f = open(path, 'r')
        new_lines = f.readlines()
        print "Length of new lines from path " + path + " " + str(len(new_lines))
        lines.extend(new_lines)
    return [[line.split(delimiter)[i].strip() for line in lines] for i in range(len(lines[0].split('|')))]


# Returns the raw lines from the txt file
def read_raw_lines(file_paths):
    lines = []
    for path in file_paths:
        f = open(path, 'r')
        new_lines = f.readlines()
        print "Length of new lines from path " + path + " " + str(len(new_lines))
        lines.extend(new_lines)
    return lines


def read_csv_file(path):
    data = []
    with open(path) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            data.append(row)
    return data

def read_csv_lines(path):
    with open(path) as csvfile:
        lines = csvfile.readlines()
        lines = [line.split('","') for line in lines]
        lines = [[elem.rstrip('\'\"\r\n') for elem in row] for row in lines]
    return lines

# Connect to the database
def connect():
    # Read configurations from file
    config = ConfigParser.ConfigParser()
    config.read(main_dir + "/dbconfig.cnf")
    database = config.get('client', 'database')
    user = config.get('client', 'user')
    password = config.get('client', 'password')
    hostname = config.get('client', 'hostname')
    port = config.get('client', 'port')
    conn = psycopg2.connect(database=database, user = user, password = password, host = hostname, port = port)
    print "Opened database " + database
    return conn


def delete_table(connection, table_name):
    # Connect to the database
    cur = connection.cursor()
    cur.execute('DROP TABLE ' + table_name)
    print "Deleted table " + table_name
    connection.commit()
    connection.close()
    return connection


def create_database(dbname):

    conn = psycopg2.connect(database="postgres", user = "mefeakengin", password = "xyz", host = "localhost", port = "5432")
    cur = conn.cursor()
    cur.execute('CREATE DATABASE ' + dbname)
    print "Database " + dbname + " is created"
    cur.close()
    

def create_candidate_table(paths):

    candidate = read_txt_files(paths, "|")

    # Positions from the data read according to the headers
    candidate.insert(0, [])
    CAND_ID_POS = 1
    CAND_NAME_POS = 2
    CAND_PTY_AFFILIATION_POS = 3
    CAND_ELECTION_YEAR_POS = 4
    CAND_OFFICE_STATE_POS = 5
    CAND_OFFICE_POS = 6
    CAND_OFFICE_DISTRICT_POS = 7
    CAND_ICI_POS = 8 #Incumber challenger status
    CAND_STATUS_POS = 9 
    CAND_COMMITTEE_POS = 10 #principal campaign committee
    CAND_STREET1_POS = 11
    CAND_STREET2_POS = 12
    CAND_CITY_POS = 13
    CAND_STATE_POS = 14
    CAND_ZIP_POS = 15

    # Connect to the database
    conn = connect()
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS CANDIDATE;")
    cur.execute('''CREATE TABLE CANDIDATE
          (ID               CHAR(9)                 NOT NULL,
          NAME              TEXT                    NOT NULL,
          PTY_AFFILIATION   CHAR(3)                 NOT NULL,
          ELECTION_YEAR     REAL,
          OFFICE_STATE      CHAR(2),
          OFFICE            CHAR(1),
          OFFICE_DISTRICT   CHAR(2),
          ICI               CHAR(1),
          STATUS            CHAR(1),
          COMMITTEE         CHAR(9),
          STREET1           CHAR(34),
          STREET2           CHAR(34),
          CITY              CHAR(30),
          STATE             CHAR(2),
          ZIP               CHAR(9));''')
    print "Table created successfully"

    conn.commit()

    for i in range(len(candidate[1])):
        CAND_ID = candidate[CAND_ID_POS][i]
        CAND_NAME = candidate[CAND_NAME_POS][i]
        CAND_PTY_AFFILIATION = candidate[CAND_PTY_AFFILIATION_POS][i]
        CAND_ELECTION_YEAR = candidate[CAND_ELECTION_YEAR_POS][i]
        CAND_OFFICE_STATE = candidate[CAND_OFFICE_STATE_POS][i]
        CAND_OFFICE = candidate[CAND_OFFICE_POS][i]
        CAND_OFFICE_DISTRICT = candidate[CAND_OFFICE_DISTRICT_POS][i]
        CAND_ICI = candidate[CAND_ICI_POS][i]
        CAND_STATUS = candidate[CAND_STATUS_POS][i]
        CAND_COMMITTEE = candidate[CAND_COMMITTEE_POS][i] #principal campaign committee
        CAND_STREET1 = candidate[CAND_STREET1_POS][i]
        CAND_STREET2 = candidate[CAND_STREET2_POS][i]
        CAND_CITY = candidate[CAND_CITY_POS][i]
        CAND_STATE = candidate[CAND_STATE_POS][i]
        CAND_ZIP = candidate[CAND_ZIP_POS][i].strip()
        
        values_list = [CAND_ID, CAND_NAME, CAND_PTY_AFFILIATION, CAND_ELECTION_YEAR,
            CAND_OFFICE_STATE, CAND_OFFICE, CAND_OFFICE_DISTRICT, 
            CAND_ICI, CAND_STATUS, CAND_COMMITTEE, CAND_STREET1,
            CAND_STREET2, CAND_CITY, CAND_STATE, CAND_ZIP]
        query = "INSERT INTO CANDIDATE (ID,NAME,PTY_AFFILIATION,ELECTION_YEAR, \
            OFFICE_STATE,OFFICE,OFFICE_DISTRICT,ICI,STATUS,COMMITTEE, \
            STREET1, STREET2, CITY, STATE, ZIP) \
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cur.execute(query, values_list)

    conn.commit()
    conn.close()


def create_committee_table(paths):

    committee = read_txt_files(paths)

    # Connect to the database
    conn = connect()
    cur = conn.cursor()

    # Positions according to the header file
    committee.insert(0, [])
    CMTE_ID_POS = 1
    CMTE_NAME_POS = 2
    TRES_NAME_POS = 3
    CMTE_STREET1_POS = 4
    CMTE_STREET2_POS = 5
    CMTE_CITY_POS = 6
    CMTE_STATE_POS = 7
    CMTE_ZIP_POS = 8
    CMTE_DSGN_POS = 9
    CMTE_TYPE_POS = 10
    CMTE_PTY_AFFILIATION_POS = 11
    CMTE_FILING_FREQ_POS = 12
    ORG_TYPE_POS = 13
    CONNECTED_ORG_NAME_POS = 14
    CAND_ID_POS = 15

    cur.execute("DROP TABLE IF EXISTS COMMITTEE;")
    cur.execute('''CREATE TABLE COMMITTEE
          (ID                   CHAR(9)                 NOT NULL,
          NAME                  TEXT                    NOT NULL,
          TREASURER             CHAR(90),
          STREET1               CHAR(34),
          STREET2               CHAR(34),
          CITY                  CHAR(30),
          STATE                 CHAR(2),
          ZIP                   CHAR(9),
          DESIGNATION           CHAR(1)                 NOT NULL,
          TYPE                  CHAR(1),
          PARTY                 CHAR(3)                 NOT NULL,
          FILING_FREQ           CHAR(1),
          ORG_TYPE              CHAR(1),
          CONNECTED_ORG_NAME    CHAR(200),
          CANDIDATE_ID          CHAR(9));''')
    print "Table created successfully"

    for i in range(len(committee[1])):
        ID = committee[CMTE_ID_POS][i]
        NAME = committee[CMTE_NAME_POS][i]
        TREASURER = committee[TRES_NAME_POS][i]
        CMTE_STREET1 = committee[CMTE_STREET1_POS][i]
        CMTE_STREET2 = committee[CMTE_STREET2_POS][i]
        CMTE_CITY = committee[CMTE_CITY_POS][i]
        CMTE_STATE = committee[CMTE_STATE_POS][i]
        CMTE_ZIP = committee[CMTE_ZIP_POS][i]
        CMTE_DSGN = committee[CMTE_DSGN_POS][i]
        CMTE_TYPE = committee[CMTE_TYPE_POS][i]
        CMTE_PTY_AFFILIATION = committee[CMTE_PTY_AFFILIATION_POS][i]
        CMTE_FILING_FREQ = committee[CMTE_FILING_FREQ_POS][i]
        ORG_TYPE = committee[ORG_TYPE_POS][i]
        CONNECTED_ORG_NAME = committee[CONNECTED_ORG_NAME_POS][i]
        CAND_ID = committee[CAND_ID_POS][i]

        values_list = [ID, NAME, TREASURER, CMTE_STREET1, CMTE_STREET2, CMTE_CITY,
                      CMTE_STATE, CMTE_ZIP, CMTE_DSGN, CMTE_TYPE, CMTE_PTY_AFFILIATION,
                      CMTE_FILING_FREQ, ORG_TYPE, CONNECTED_ORG_NAME, CAND_ID]
        query = "INSERT INTO COMMITTEE (ID,NAME,TREASURER,STREET1,STREET2,CITY,STATE,\
                ZIP, DESIGNATION, TYPE, PARTY, FILING_FREQ, ORG_TYPE, CONNECTED_ORG_NAME,\
                CANDIDATE_ID) \
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cur.execute(query, values_list)

        if i%10000 == 0:
            print i

    conn.commit()
    conn.close()


def create_individual_contribution_table(paths):
    '''Create the Individual Contribution Table'''

    # Connect to the database
    conn = connect()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS INDIVIDUAL;")
    cur.execute('''CREATE TABLE INDIVIDUAL
          (COMMITTEE_ID             CHAR(9)  		     	NOT NULL,
          AMNDT_IND                 CHAR(1),
          REPORT_TYPE               CHAR(3),
          TRAN_PGI                  CHAR(5),
          IMAGE_NUM                 CHAR(18),
          TRAN_TYPE                 CHAR(3),
          ENTITY_TYPE               CHAR(3),
          NAME                      TEXT	     			NOT NULL,
          CITY                      TEXT	     			NOT NULL,
          STATE                     CHAR(2)	     			NOT NULL,
          ZIP                       CHAR(9)	     			NOT NULL,
          EMPLOYER                  TEXT,
          OCCUPATION                TEXT,
          TRAN_DATE		    		DATE,
          TRAN_AMOUNT				NUMERIC(14,2),
          OTHER_ID                  CHAR(9),
          TRAN_ID                   CHAR(32),
          FILE_NUM                  CHAR(22),
          MEMO_CODE                 CHAR(1),
          MEMO_TEXT                 CHAR(100),
          SUB_ID                    CHAR(19));''')
    #           TRAN_DATE_AVAILABLE		NUMERIC(1,2)
    print "Table created successfully"

    dateless_count = 0
    for path in paths:
        # election_cycle = path[len(path)-6:len(path)-4] # Assuming the path ends in format like ../indiv90.txt
        cur = conn.cursor()

        print "Files are read from ", path
        raw_lines = read_raw_lines([path])

        for i in range(len(raw_lines)):
            line_raw = raw_lines[i]
            row = line_raw.split("|")
            row = [elem.strip() for elem in row]

            TRAN_DATE_POS = 14  # The position of transaction date on data
            TRAN_DATE_POS -= 1  # Account for array shift by one
            if len(row[TRAN_DATE_POS]) < 8:  # In case there is no data
                dateless_count += 1
                continue

            row[TRAN_DATE_POS] = str(row[TRAN_DATE_POS][4:]) + '-' + \
                                 str(row[TRAN_DATE_POS][0:2]) + '-' + \
                                 str(row[TRAN_DATE_POS][2:4])

            query = "INSERT INTO INDIVIDUAL \
                (COMMITTEE_ID,AMNDT_IND,REPORT_TYPE,TRAN_PGI,IMAGE_NUM,TRAN_TYPE,\
                ENTITY_TYPE,NAME,CITY,STATE,ZIP,EMPLOYER,OCCUPATION,TRAN_DATE,TRAN_AMOUNT,\
                OTHER_ID,TRAN_ID,FILE_NUM,MEMO_CODE,MEMO_TEXT,SUB_ID) \
                VALUES (%s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s,\
                        %s)"
            cur.execute(query, row)

            if i % 100000 == 0:
                print i
                print "dateless count, ", dateless_count

        print "Insertion for the file completed: ", path

    conn.commit()
    conn.close()


def create_committee_candidate_contribution_table(paths):

    '''Create the Candidate Committee Contribution'''
    # Connect to the database
    conn = connect()
    cur = conn.cursor()

    # Positions according to the header file
    COMMITTEE_ID_POS = 1
    AMNDT_IND_POS = 2
    REPORT_TYPE_POS = 3
    TRAN_PGI_POS = 4  # indicates the election info and year for which the contribution was made.
    IMAGE_NUM_POS = 5
    TRAN_TYPE_POS = 6
    ENTITY_TYPE_POS = 7
    NAME_POS = 8
    CITY_POS = 9
    STATE_POS = 10
    ZIP_POS = 11
    EMPLOYER_POS = 12
    OCCUPATION_POS = 13
    TRAN_DATE_POS = 14
    TRAN_AMOUNT_POS = 15
    OTHER_ID_POS = 16 # FEC ID of the recipient committee or the supported or opposed candidate ID.
    CANDIDATE_ID_POS = 17 # recipient candidate ID
    TRAN_ID_POS = 18
    FILE_NUM_POS = 19
    MEMO_CODE_POS = 20
    MEMO_TEXT_POS = 21
    SUB_ID_POS = 22  # FEC Record Number

    cur.execute("DROP TABLE IF EXISTS COMMITTEE_TO_CANDIDATE;")
    cur.execute('''CREATE TABLE COMMITTEE_TO_CANDIDATE
          (COMMITTEE_ID             CHAR(9)  		     	NOT NULL,
          AMNDT_IND                 CHAR(1),
          REPORT_TYPE               CHAR(3),
          TRAN_PGI                  CHAR(5),
          IMAGE_NUM                 CHAR(18),
          TRAN_TYPE                 CHAR(3),
          ENTITY_TYPE               CHAR(3),
          NAME                      TEXT	     			NOT NULL,
          CITY                      TEXT	     			NOT NULL,
          STATE                     CHAR(2)	     			NOT NULL,
          ZIP                       CHAR(9)	     			NOT NULL,
          EMPLOYER                  TEXT,
          OCCUPATION                TEXT,
          TRAN_DATE		    		DATE,
          TRAN_AMOUNT				NUMERIC(14,2),
          OTHER_ID                  CHAR(9),
          CANDIDATE_ID              CHAR(9),
          TRAN_ID                   CHAR(32),
          FILE_NUM                  CHAR(22),
          MEMO_CODE                 CHAR(1),
          MEMO_TEXT                 CHAR(100),
          SUB_ID                    CHAR(19));''')
    print "Table created successfully"
    conn.commit()
    conn.close()

    dateless_count = 0 #Count how many of the lines don't have a transaction date
    for path in paths:
        conn = connect()
        cur = conn.cursor()
        committee_to_candidate = read_txt_files([path], "|")
        committee_to_candidate.insert(0, [])
        for i in range(len(committee_to_candidate[1])):
            COMMITTEE_ID = committee_to_candidate[COMMITTEE_ID_POS][i]
            AMNDT_IND = committee_to_candidate[AMNDT_IND_POS][i]
            REPORT_TYPE = committee_to_candidate[REPORT_TYPE_POS][i]
            TRAN_PGI = committee_to_candidate[TRAN_PGI_POS][i]
            IMAGE_NUM = committee_to_candidate[IMAGE_NUM_POS][i]
            TRAN_TYPE = committee_to_candidate[TRAN_TYPE_POS][i]
            ENTITY_TYPE = committee_to_candidate[ENTITY_TYPE_POS][i]
            NAME = committee_to_candidate[NAME_POS][i]
            CITY = committee_to_candidate[CITY_POS][i]
            STATE = committee_to_candidate[STATE_POS][i]
            ZIP = committee_to_candidate[ZIP_POS][i]
            EMPLOYER = committee_to_candidate[EMPLOYER_POS][i]
            OCCUPATION = committee_to_candidate[OCCUPATION_POS][i]

            if len(committee_to_candidate[TRAN_DATE_POS][i]) == 0:  # In case there is no data
                dateless_count += 1
                continue
            TRAN_DATE = str(committee_to_candidate[TRAN_DATE_POS][i][4:]) + '-' + \
                        str(committee_to_candidate[TRAN_DATE_POS][i][0:2]) + '-' + str(
                committee_to_candidate[TRAN_DATE_POS][i][2:4])

            TRAN_AMOUNT = committee_to_candidate[TRAN_AMOUNT_POS][i]
            OTHER_ID = committee_to_candidate[OTHER_ID_POS][i]
            CANDIDATE_ID = committee_to_candidate[CANDIDATE_ID_POS][i]
            TRAN_ID = committee_to_candidate[TRAN_ID_POS][i]
            FILE_NUM = committee_to_candidate[FILE_NUM_POS][i]
            MEMO_CODE = committee_to_candidate[MEMO_CODE_POS][i]
            MEMO_TEXT = committee_to_candidate[MEMO_TEXT_POS][i]
            SUB_ID = committee_to_candidate[SUB_ID_POS][i]

            values_list = [COMMITTEE_ID, AMNDT_IND, REPORT_TYPE, TRAN_PGI, IMAGE_NUM,
                           TRAN_TYPE, ENTITY_TYPE, NAME, CITY, STATE, ZIP, EMPLOYER,
                           OCCUPATION, TRAN_DATE, TRAN_AMOUNT, OTHER_ID, CANDIDATE_ID,
                           TRAN_ID, FILE_NUM, MEMO_CODE, MEMO_TEXT, SUB_ID]

            query = "INSERT INTO COMMITTEE_TO_CANDIDATE \
                (COMMITTEE_ID,AMNDT_IND,REPORT_TYPE,TRAN_PGI,IMAGE_NUM,\
                TRAN_TYPE,ENTITY_TYPE,NAME,CITY,STATE,\
                ZIP,EMPLOYER,OCCUPATION,TRAN_DATE,TRAN_AMOUNT,\
                OTHER_ID,CANDIDATE_ID,TRAN_ID,FILE_NUM,MEMO_CODE,\
                MEMO_TEXT,SUB_ID) \
                VALUES (%s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s,\
                        %s, %s)"
            cur.execute(query, values_list)

            if i%100000 == 0:
                print i

        conn.commit()
        conn.close()

        print "Rows without dates are removed. Number of rows without a date is: " + str(dateless_count)
        print "Insertion for the file completed: ", path
        

def create_committee_committee_table(paths):

    '''Create the Candidate Committee Contribution'''
#     CMTE_CONTRIBUTED_ID_POS = 16
#     CMTE_CONTRIBUTOR_ID_POS = 1
#     CMTE_CONTRIBUTOR_NAME_POS = 8
#     TRAN_DATE_POS = 14
#     TRAN_AMOUNT_POS = 15
#     TRAN_TYPE_POS = 6

    CONTRIBUTOR_ID_POS = 1
    AMNDT_IND_POS = 2
    REPORT_TYPE_POS = 3
    TRAN_PGI_POS = 4 #indicates the election for which the contribution was made.
    IMAGE_NUM_POS = 5
    TRAN_TYPE_POS = 6
    ENTITY_TYPE_POS = 7
    NAME_POS = 8
    CITY_POS = 9
    STATE_POS = 10
    ZIP_POS = 11
    EMPLOYER_POS = 12
    OCCUPATION_POS = 13
    TRAN_DATE_POS = 14
    TRAN_AMOUNT_POS = 15
    CONTRIBUTED_ID_POS = 16
    TRAN_ID_POS = 17
    FILE_NUM_POS = 18
    MEMO_CODE_POS = 19
    MEMO_TEXT_POS = 20
    SUB_ID_POS = 21 #FEC Record Number

    # Connect to the database
    conn = connect()
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS COMMITTEE_TO_COMMITTEE;")
    cur.execute('''CREATE TABLE COMMITTEE_TO_COMMITTEE
          (CONTRIBUTOR_ID           CHAR(9)  		     	NOT NULL,
          AMNDT_IND                 CHAR(1),
          REPORT_TYPE               CHAR(3),
          TRAN_PGI                  CHAR(5),
          IMAGE_NUM                 CHAR(18),
          TRAN_TYPE                 CHAR(3),
          ENTITY_TYPE               CHAR(3),
          NAME                      TEXT	     			NOT NULL,
          CITY                      TEXT	     			NOT NULL,
          STATE                     CHAR(2)	     			NOT NULL,
          ZIP                       CHAR(9)	     			NOT NULL,
          EMPLOYER                  TEXT,
          OCCUPATION                TEXT,
          TRAN_DATE		    		DATE,
          TRAN_AMOUNT				NUMERIC(14,2),
          CONTRIBUTED_ID            CHAR(9),
          TRAN_ID                   CHAR(32),
          FILE_NUM                  CHAR(22),
          MEMO_CODE                 CHAR(1),
          MEMO_TEXT                 CHAR(100),
          SUB_ID                    CHAR(19));''')
    print "Table created successfully"
    conn.commit()
    conn.close()

    dateless_count = 0 #Count how many of the lines don't have a transaction date
    for path in paths:	
        conn = connect()
        cur = conn.cursor()
        committee_to_committee = read_txt_files([path], "|")
        committee_to_committee.insert(0, [])

        for i in range(len(committee_to_committee[1])):
            CONTRIBUTOR_ID = committee_to_committee[CONTRIBUTOR_ID_POS][i]
            AMNDT_IND = committee_to_committee[AMNDT_IND_POS][i]
            REPORT_TYPE = committee_to_committee[REPORT_TYPE_POS][i]
            TRAN_PGI = committee_to_committee[TRAN_PGI_POS][i]
            IMAGE_NUM = committee_to_committee[IMAGE_NUM_POS][i]
            TRAN_TYPE = committee_to_committee[TRAN_TYPE_POS][i]
            ENTITY_TYPE = committee_to_committee[ENTITY_TYPE_POS][i]
            NAME = committee_to_committee[NAME_POS][i]
            CITY = committee_to_committee[CITY_POS][i]
            STATE = committee_to_committee[STATE_POS][i]
            ZIP = committee_to_committee[ZIP_POS][i]
            EMPLOYER = committee_to_committee[EMPLOYER_POS][i]
            OCCUPATION = committee_to_committee[OCCUPATION_POS][i]
            
            if len(committee_to_committee[TRAN_DATE_POS][i]) == 0: # In case there is no data
                dateless_count += 1
                continue
            TRAN_DATE = str(committee_to_committee[TRAN_DATE_POS][i][4:]) + '-' + \
                str(committee_to_committee[TRAN_DATE_POS][i][0:2]) + '-' + str(committee_to_committee[TRAN_DATE_POS][i][2:4])            
            
            TRAN_AMOUNT = committee_to_committee[TRAN_AMOUNT_POS][i]
            CONTRIBUTED_ID = committee_to_committee[CONTRIBUTED_ID_POS][i]
            TRAN_ID = committee_to_committee[TRAN_ID_POS][i]
            FILE_NUM = committee_to_committee[FILE_NUM_POS][i]
            MEMO_CODE = committee_to_committee[MEMO_CODE_POS][i]
            MEMO_TEXT = committee_to_committee[MEMO_TEXT_POS][i]
            SUB_ID = committee_to_committee[SUB_ID_POS][i]

            values_list = [CONTRIBUTOR_ID, AMNDT_IND, REPORT_TYPE, TRAN_PGI, IMAGE_NUM,
                           TRAN_TYPE, ENTITY_TYPE, NAME, CITY, STATE, ZIP, EMPLOYER,
                           OCCUPATION, TRAN_DATE, TRAN_AMOUNT, CONTRIBUTED_ID, TRAN_ID,
                           FILE_NUM, MEMO_CODE, MEMO_TEXT, SUB_ID]
            query = "INSERT INTO COMMITTEE_TO_COMMITTEE \
                (CONTRIBUTOR_ID,AMNDT_IND,REPORT_TYPE,TRAN_PGI,IMAGE_NUM,TRAN_TYPE,\
                ENTITY_TYPE,NAME,CITY,STATE,ZIP,EMPLOYER,OCCUPATION,TRAN_DATE,TRAN_AMOUNT,\
                CONTRIBUTED_ID,TRAN_ID,FILE_NUM,MEMO_CODE,MEMO_TEXT,SUB_ID) \
                VALUES (%s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s,\
                        %s)"

            cur.execute(query, values_list)

            if i%100000 == 0:
                print i

        conn.commit()
        conn.close()

        print "Rows without dates are removed. Number of rows without a date is: " + str(dateless_count)
        print "Insertion for the file completed: ", path


# Creates a corporate table with corporate and pac info from a csv file
def create_corporate_table(paths):

    # Connect to the database
    conn = connect()
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS CORPORATE;")
    cur.execute('''CREATE TABLE CORPORATE
          (CORPORATE_NAME       CHAR(200)              NOT NULL,
          GVKEY                 NUMERIC(14,2)           NOT NULL,
          COMMITTEE_NAME        CHAR(200)               NOT NULL,
          COMMITTEE_ID          CHAR(9)                 NOT NULL);''')
    print "Table created successfully"

    for path in paths:
        print "Files are read from ", path
        raw_lines = read_csv_file(path)
        file_header = raw_lines[0]
        raw_lines.pop(0)

        for i in range(len(raw_lines)):
            row = raw_lines[i]
            row = [elem.strip() for elem in row]

            query = "INSERT INTO CORPORATE (CORPORATE_NAME,GVKEY,COMMITTEE_NAME,COMMITTEE_ID) \
                VALUES (%s, %s, %s, %s)"
            cur.execute(query, row)

    conn.commit()
    conn.close()
    print "Corporate table is created successfully"


# Creates a corporate table with employer info from a csv file
def create_employer_table(paths):

    # Connect to the database
    conn = connect()
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS EMPLOYER;")
    cur.execute('''CREATE TABLE EMPLOYER
          (EMPLOYER_NAME        CHAR(200)               NOT NULL,
          CLEAN_NAME            CHAR(200)               NOT NULL,
          GVKEY                 NUMERIC(14,0)           NOT NULL);''')
    print "Table created successfully"

    for path in paths:
        print "Files are read from ", path
        raw_lines = read_csv_file(path)
        file_header = raw_lines[0]
        raw_lines.pop(0)

        ORIG_NAME_POS = 0
        CLEAN_NAME_POS = 1
        GVKEY_POS = 3

        for i in range(len(raw_lines)):
            row = raw_lines[i]
            row = [row[ORIG_NAME_POS], row[CLEAN_NAME_POS], row[GVKEY_POS]]
            row = [elem.strip() for elem in row]
            query = "INSERT INTO EMPLOYER (EMPLOYER_NAME,CLEAN_NAME,GVKEY) \
                VALUES (%s, %s, %s)"
            print row
            cur.execute(query, row)

    conn.commit()
    conn.close()
    print "Employer table is created successfully"


# Creates a corporate table with employer info from a csv file
def create_occupation_soc_table(paths):

    # Connect to the database
    conn = connect()
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS OCCUPATION_SOC;")
    cur.execute('''CREATE TABLE OCCUPATION_SOC
          (OCCUPATION_ORIGINAL  CHAR(200)               NOT NULL,
          OCCUPATION_CLEAN      CHAR(200)               NOT NULL,
          SOC_CODE              CHAR(7)                 NOT NULL);''')
    print "Table created successfully"

    for path in paths:
        print "Files are read from ", path
        raw_lines = read_raw_lines([path])

        ORIG_NAME_POS = 0
        CLEAN_NAME_POS = 1
        SOC_CODE_POS = 3

        rows_with_issues = 0
        for i in range(len(raw_lines)):
            line_raw = raw_lines[i]
            row = line_raw.split("\t")
            print row

            # Remove the ones that are just ''
            j = 0
            while j < len(row):
                if row[j] == '':
                    row.pop(j)
                    j -=1
                j += 1

            row = [elem.strip() for elem in row]
            if len(row) > 5:
                rows_with_issues += 1
                row = [row[ORIG_NAME_POS], row[CLEAN_NAME_POS], row[-2]]
            elif len(row) < 5:
                rows_with_issues += 1
                row = [row[ORIG_NAME_POS], row[CLEAN_NAME_POS], row[-2]]
            else:
                row = [row[ORIG_NAME_POS], row[CLEAN_NAME_POS], row[SOC_CODE_POS]]

            query = "INSERT INTO OCCUPATION_SOC (OCCUPATION_ORIGINAL,OCCUPATION_CLEAN,SOC_CODE) \
                VALUES (%s, %s, %s)"
            print row
            cur.execute(query, row)

    conn.commit()
    conn.close()
    print "occupation_soc table is created successfully"
    print "rows with issues: ", rows_with_issues


# Creates a corporate table with employer info from a csv file
def create_occupation_soc_onet_table(paths):

    # Connect to the database
    conn = connect()
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS OCCUPATION_SOC_ONET;")
    cur.execute('''CREATE TABLE OCCUPATION_SOC_ONET
          (OCCUPATION_ORIGINAL  CHAR(200)               NOT NULL,
          OCCUPATION_CLEAN      CHAR(200)               NOT NULL,
          SOC_ONET_CODE          CHAR(10)              NOT NULL);''')
    print "Table created successfully"

    for path in paths:
        print "Files are read from ", path
        raw_lines = read_raw_lines([path])

        ORIG_NAME_POS = 0
        CLEAN_NAME_POS = 1
        SOC_ONET_CODE_POS = 3

        rows_with_issues = 0
        for i in range(len(raw_lines)):
            line_raw = raw_lines[i]
            row = line_raw.split("\t")
            print row

            # Remove the ones that are just ''
            j = 0
            while j < len(row):
                if row[j] == '':
                    row.pop(j)
                    j -=1
                j += 1

            row = [elem.strip() for elem in row]
            if len(row) > 5:
                rows_with_issues += 1
                row = [row[ORIG_NAME_POS], row[CLEAN_NAME_POS], row[-2]]
            elif len(row) < 5:
                rows_with_issues += 1
                row = [row[ORIG_NAME_POS], row[CLEAN_NAME_POS], row[-2]]
            else:
                row = [row[ORIG_NAME_POS], row[CLEAN_NAME_POS], row[SOC_ONET_CODE_POS]]

            query = "INSERT INTO OCCUPATION_SOC_ONET (OCCUPATION_ORIGINAL,OCCUPATION_CLEAN,SOC_ONET_CODE) \
                VALUES (%s, %s, %s)"
            print row
            cur.execute(query, row)

    conn.commit()
    conn.close()
    print "occupation_soc_onet table is created successfully"
    print "rows with issues: ", rows_with_issues #Only two rows with issues anyways!

# This table includes an individual ID assignment,
def create_individual_updated_table(paths):
    '''Create the Individual Contribution Table'''

    # Connect to the database
    conn = connect()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS INDIVIDUAL_UPDATED;")
    cur.execute('''CREATE TABLE INDIVIDUAL_UPDATED
          (COMMITTEE_ID             CHAR(9)  		     	NOT NULL,
          AMNDT_IND                 CHAR(1),
          REPORT_TYPE               CHAR(3),
          TRAN_PGI                  CHAR(5),
          IMAGE_NUM                 CHAR(18),
          TRAN_TYPE                 CHAR(3),
          ENTITY_TYPE               CHAR(3),
          INDIVIDUAL_ID             CHAR(9)                 NOT NULL,
          ORIGINAL_NAME             TEXT	     			NOT NULL,
          FULL_NAME                 TEXT,
          FIRST_NAME                TEXT,
          LAST_NAME                 TEXT,
          CITY                      TEXT,
          STATE                     CHAR(2),
          ZIP                       CHAR(9),
          EMPLOYER                  TEXT,
          OCCUPATION                TEXT,
          TRAN_DATE		    		DATE,
          TRAN_AMOUNT				NUMERIC(14,2),
          OTHER_ID                  CHAR(9),
          TRAN_ID                   CHAR(32),
          FILE_NUM                  CHAR(22),
          MEMO_CODE                 CHAR(1),
          MEMO_TEXT                 CHAR(100),
          SUB_ID                    CHAR(19));''')
    #           TRAN_DATE_AVAILABLE		NUMERIC(1,2)
    print "Table created successfully"

    dateless_count = 0
    individual_id_count = 1 # We will create individual ids with this count

    for path in paths:
        # election_cycle = path[len(path)-6:len(path)-4] # Assuming the path ends in format like ../indiv90.txt
        cur = conn.cursor()

        print "Files are read from ", path
        raw_lines = read_raw_lines([path])

        for ix in range(len(raw_lines)):
            line_raw = raw_lines[ix]
            row = line_raw.split("|")
            row = [elem.strip() for elem in row]

            ENTITY_TYPE_IDX = 6
            INDIVIDUAL_ID_IDX = 7
            ORIGINAL_NAME_IDX = 8
            FULL_NAME_IDX = 9
            FIST_NAME_IDX = 10
            LAST_NAME_IDX = 11
            TRAN_DATE_IDX = 17  # The position of transaction date on data

            individual_id = ''
            full_name = ''
            first_name = ''
            last_name = ''
            row.insert(INDIVIDUAL_ID_IDX, individual_id)
            row.insert(FULL_NAME_IDX, full_name)
            row.insert(FIST_NAME_IDX, first_name)
            row.insert(LAST_NAME_IDX, last_name)

            entity_type = row[ENTITY_TYPE_IDX]

            # In cases when it is an individual
            original_name = row[ORIGINAL_NAME_IDX]
            if (entity_type == 'IND' or entity_type == '') and original_name != '':
                individual_id = format(individual_id_count, '08d')
                individual_id = 'I' + individual_id
                row[INDIVIDUAL_ID_IDX] = individual_id
                individual_id_count += 1

                name_split = original_name.split('.')
                name = ''.join(name_split)

                r = re.compile('MR|MRS|MS|MISS|MD|JD|PHD|LCC')  # First remove initials
                name_split = name.split(' ')
                name_filtered = filter(lambda x: len(filter(r.match, [x])) == 0, name_split)
                name = ' '.join(name_filtered)

                name_split = name.split(', ')  # 'Last_name, First_name' or "First_name Last_name" case
                if len(name_split) > 1:
                    last_name = name_split.pop(0)
                elif len(name.split(' ')) > 1:
                    name_split = name.split(' ')
                    last_name = name_split.pop(-1)

                first_name = ' '.join(name_split)
                first_name_split = first_name.split('.|,|!|\\"|?')  # Remove extra punctuation ,
                first_name = ' '.join(first_name_split)
                first_name = first_name.strip(' ')

                last_name_split = last_name.split(',')
                last_name = ' '.join(last_name_split)
                last_name = last_name.strip(' ')

                full_name = first_name + ' ' + last_name

            row[FULL_NAME_IDX] = full_name
            row[FIST_NAME_IDX] = first_name
            row[LAST_NAME_IDX] = last_name

            if len(row[TRAN_DATE_IDX]) < 8:  # In case there is no data
                print "no transaction date, ", row[TRAN_DATE_IDX]
                dateless_count += 1
                continue

            row[TRAN_DATE_IDX] = str(row[TRAN_DATE_IDX][4:]) + '-' + \
                                 str(row[TRAN_DATE_IDX][0:2]) + '-' + \
                                 str(row[TRAN_DATE_IDX][2:4])

            query = "INSERT INTO INDIVIDUAL_UPDATED \
                (COMMITTEE_ID, AMNDT_IND, REPORT_TYPE, TRAN_PGI, IMAGE_NUM, \
                TRAN_TYPE, ENTITY_TYPE, INDIVIDUAL_ID, ORIGINAL_NAME, FULL_NAME, \
                FIRST_NAME, LAST_NAME, CITY, STATE,ZIP, \
                EMPLOYER, OCCUPATION,TRAN_DATE,TRAN_AMOUNT, OTHER_ID, \
                TRAN_ID, FILE_NUM, MEMO_CODE, MEMO_TEXT, SUB_ID) \
                VALUES (%s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s)"
            cur.execute(query, row)

            if ix % 100000 == 0:
                print ix
                print "dateless count, ", dateless_count
                print individual_id
                print row

            # print ix

        print "Insertion for the file completed: ", path

    conn.commit()
    conn.close()

# #Creating Tables
# candidate_paths = [(data_dir + 'cn' + str(i)[2:] + '.txt') for i in range(1980, 2020, 2)]
# create_candidate_table(candidate_paths)
# #
# committee_paths = [(data_dir + 'cm' + str(i)[2:] + '.txt') for i in range(1980, 2020, 2)]
# create_committee_table(committee_paths)
#
# individual_paths = [(data_dir + 'indiv' + str(i)[2:] + '.txt') for i in range(1980, 2020, 2)]
# create_individual_contribution_table(individual_paths)
#
# committee_candidate_paths = [(data_dir + 'pas2' + str(i)[2:] + '.txt') for i in range(1980, 2020, 2)]
# create_committee_candidate_contribution_table(committee_candidate_paths)

# committee_committee_paths = [(data_dir + 'oth' + str(i)[2:] + '.txt') for i in range(1980, 2020, 2)]
# create_committee_committee_table(committee_committee_paths)

# corporate_path = '../data' + '/corporate.csv'
# create_corporate_table([corp_pac_path])

# employer_path = '../data' + '/empl_1980_2014_gvkey_match_clean.csv'
# create_employer_table([employer_path])

# occupation_soc_path = '../data' + '/fec_occ_soc_2016.txt'
# create_occupation_soc_table([occupation_soc_path])

# occupation_soc_ocnet_path = '../data' + '/fec_occ_soc_onet_2016.txt'
# create_occupation_soc_onet_table([occupation_soc_ocnet_path])

individual_paths = [(data_dir + 'indiv' + str(i)[2:] + '.txt') for i in range(1980, 2020, 2)]
create_individual_updated_table(individual_paths)
