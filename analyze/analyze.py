### The data is fetched from the raw FEC files.

import ConfigParser
import psycopg2
import os
import csv
import sys
main_dir = os.getcwd()
data_dir = '../data/fec/'


def read_files(file_paths):
    lines = []
    for path in file_paths:
        f = open(path, 'r')
        new_lines = f.readlines()
        # print "Length of new lines from path " + path + " " + str(len(new_lines))
        lines.extend(new_lines)
    return [line.split('|') for line in lines]


def read_data(paths, columns):
    raw = read_files(paths)
    data = [[row[column] for column in columns] for row in raw]
    return data


def read_csv_file(path):
    data = []
    with open(path) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            data.append(row)
    return data


'''Committee Data'''
CMTE_ID_POS = 0
CMTE_NAME_POS = 1
CMTE_DSGN_POS = 8
CMTE_TP_POS = 9
CMTE_PTY_AFFILIATION_POS = 10
ORG_TP_POS = 12

committee_paths = [(data_dir + 'cm' + str(i)[2:] + '.txt') for i in range(1980, 2020, 2)]
committee_data = read_files(committee_paths)
print "Committee data is loaded"


'''Committee To Committee Data'''
CMTE_TO_CMTE_CONTRIBUTED_ID_POS = 15
CMTE_TO_CMTE_CONTRIBUTOR_ID_POS = 0
CMTE_TO_CMTE_CONTRIBUTOR_NAME_POS = 7
CMTE_TO_CMTE_TRAN_DATE_POS = 13
CMTE_TO_CMTE_TRAN_AMOUNT_POS = 14
CMTE_TO_CMTE_TRAN_TYPE_POS = 5

committee_committee_paths = [(data_dir + 'pas2' + str(i)[2:] + '.txt') for i in range(1980, 2020, 2)]
committee_committee_data = read_files(committee_committee_paths)
print "Committee to committee data is loaded"


'''Corporate Data'''
COPR_NAME_POS = 0
CORP_CMTE_ID_POS = 3

corporate_path = '../data' + '/corporate.csv'
corporate_data = read_csv_file(corporate_path)
corporate_header = corporate_path[0]
corporate_data.pop(0)
print "Corporate data data is loaded"


# Pick a few corporations and examine their contributions
def examine_corporate_committee_contribution(corp_cmte_id):
    corp_cmte_id = 'C00236034'
    cmte_conts = filter(lambda x: x[CMTE_TO_CMTE_CONTRIBUTOR_ID_POS] == corp_cmte_id, committee_committee_data)
    # print cmte_conts[0:3]

    contributed_ids = [cont[CMTE_TO_CMTE_CONTRIBUTED_ID_POS] for cont in cmte_conts]
    contributed_parties = []
    for cont_id in contributed_ids:
        party = filter(lambda x: x[CMTE_ID_POS] == cont_id, committee_data)
        if party:
            contributed_parties.append(party[0][CMTE_PTY_AFFILIATION_POS])
        else:
            contributed_parties.append('')

    rep_conts = []
    dem_conts = []
    for i in range(len(cmte_conts)):
        if contributed_parties[i] == 'REP':
            rep_conts.append(cmte_conts[i])
        elif contributed_parties[i] == 'DEM':
            dem_conts.append(cmte_conts[i])
    # print "dem_conts ", dem_conts
    # print "rep_conts ", rep_conts

    # add up the contributions
    def aggregate_contributions(conts):
        conts_tran_values = [int(cont[CMTE_TO_CMTE_TRAN_AMOUNT_POS]) for cont in conts]
        return sum(conts_tran_values)

    rep_cont = aggregate_contributions(rep_conts)
    dem_cont = aggregate_contributions(dem_conts)
    corp_ccr = [float(dem_cont), float(rep_cont)]
    if rep_cont + dem_cont > 0:
        corp_ccr_normalized = [x / (rep_cont + dem_cont) for x in corp_ccr]
    else:
        corp_ccr_normalized = [0, 0]
    return corp_ccr, corp_ccr_normalized


'''Calculate Corporate Contribution'''
corporate_dict = {}
for corp in corporate_data:
    if corp[CMTE_ID_POS]:
        corporate_dict[corp[CMTE_ID_POS]] = corp

print "examines corp_contribution for C00236034: ", examine_corporate_committee_contribution('C00236034')

# corp_contribution_data = []
# for cmte_id in corporate_dict.keys():
#     corp = corporate_dict[cmte_id]
#     cont_absolute, cont_normalized = examine_corporate_committee_contribution(cmte_id)
#     cmte_cont = [corp[COPR_NAME_POS], corp[CORP_CMTE_ID_POS], cont_absolute, cont_normalized]
#     corp_contribution_data.append(cmte_cont)
