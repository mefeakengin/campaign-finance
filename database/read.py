import psycopg2
data_dir = '../data/fec/'

# Read files:
def read_files(file_paths):
	lines = []
	for path in file_paths:
		f = open(path, 'r')
		new_lines = f.readlines()
		print "Length of new lines from path " + path + " " + str(len(new_lines))
		lines.extend(new_lines)
	return [[line.split('|')[i] for line in lines] for i in range(len(lines[0].split('|')))]

# Connect to the database
def connect():
	# Connect to the database
	conn = psycopg2.connect(database="testdb", user = "mefeakengin", password = "xyz", host = "localhost", port = "5432")
	print "Opened database successfully"
	return conn


def delete_table(connection, table_name):
	# Connect to the database
	cur = connection.cursor()
	cur.execute('DROP TABLE ' + table_name)
	print "Deleted table successfully"
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
	
	candidate = read_files(paths)

	# Positions from the data read according to the headers
	candidate.insert(0, [])
	CAND_ID_POS = 1
	CAND_NAME_POS = 2
	CAND_PTY_AFFILIATION_POS = 3
	CAND_ELECTION_YR_POS = 4
	CAND_OFFICE_POS = 6
	CAND_OFFICE_DISTRICT_POS = 7
	CAND_STATUS_POS = 9
	CAND_COMMITTEE_POS = 10 #principal campaign committee

	# Connect to the database
	conn = connect()
	cur = conn.cursor()

	cur.execute("DROP TABLE IF EXISTS CANDIDATE;")
	cur.execute('''CREATE TABLE CANDIDATE
	      (ID 				CHAR(9)  		    	NOT NULL,
	      NAME           	TEXT    				NOT NULL,
	      PTY_AFFILIATION 	CHAR(3)     			NOT NULL,
	      ELECTION_YR      	REAL,
	      OFFICE          	CHAR(1),
	      OFFICE_DISTRICT 	CHAR(2),
	      STATUS 			CHAR(1),
	      COMMITTEE 		CHAR(9));''')
	print "Table created successfully"

	conn.commit()

	for i in range(len(candidate[1])):
		CAND_ID = candidate[CAND_ID_POS][i]
		CAND_NAME = candidate[CAND_NAME_POS][i]
		CAND_PTY_AFFILIATION = candidate[CAND_PTY_AFFILIATION_POS][i]
		CAND_ELECTION_YR = candidate[CAND_ELECTION_YR_POS][i]
		CAND_OFFICE = candidate[CAND_OFFICE_POS][i]
		CAND_OFFICE_DISTRICT = candidate[CAND_OFFICE_DISTRICT_POS][i]
		CAND_STATUS = candidate[CAND_STATUS_POS][i]
		CAND_COMMITTEE = candidate[CAND_COMMITTEE_POS][i] #principal campaign committee

		values_list = [CAND_ID, CAND_NAME, CAND_PTY_AFFILIATION, CAND_ELECTION_YR,
			CAND_OFFICE, CAND_OFFICE_DISTRICT, CAND_STATUS, CAND_COMMITTEE]
		query = "INSERT INTO CANDIDATE (ID,NAME,PTY_AFFILIATION,ELECTION_YR, \
			OFFICE,OFFICE_DISTRICT,STATUS, COMMITTEE) \
	      	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
		cur.execute(query, values_list)

	conn.commit()
	conn.close()


def create_committee_table(paths):
	
	committee = read_files(paths)

	# Connect to the database
	conn = connect()
	cur = conn.cursor()

	# Positions according to the header file
	committee.insert(0, [])
	CMTE_ID_POS = 1
	CMTE_NM_POS = 2
	CMTE_DSGN = 9
	CMTE_TP = 10
	CMTE_PTY_AFFILIATION = 11
	ORG_TP = 13

	cur.execute("DROP TABLE IF EXISTS COMMITTEE;")
	cur.execute('''CREATE TABLE COMMITTEE
	      (ID 					CHAR(9)  		     	NOT NULL,
	      NAME           			TEXT    				NOT NULL,
	      DESIGNATION	 			CHAR(1)     			NOT NULL,
	      TYPE		      		CHAR(1),
	      PARTY			 		CHAR(3)					NOT NULL,
	      ORG_TYPE 				CHAR(1));''')
	print "Table created successfully"

	for i in range(len(committee[1])):
		ID = committee[CMTE_ID_POS][i]
		NAME = committee[CMTE_NM_POS][i]
		DESIGNATION = committee[CMTE_DSGN][i]
		TYPE = committee[CMTE_TP][i]
		PARTY = committee[CMTE_PTY_AFFILIATION][i]
		ORG_TYPE = committee[ORG_TP][i]
		values_list = [ID, NAME, DESIGNATION, TYPE, PARTY, ORG_TYPE]
		query = "INSERT INTO COMMITTEE (ID,NAME,DESIGNATION,TYPE,PARTY,ORG_TYPE) \
	      	VALUES (%s, %s, %s, %s, %s, %s)"
		cur.execute(query, values_list)

		if i%10000 == 0:
			print i

	conn.commit()
	conn.close()


def create_individual_contribution_table(paths):

	'''Create the Individual Contribution Table'''

	# Positions according to the header file
	# INDV_ID: TODO: Can we have an individual ID at some point
	NAME_POS = 8
	CITY_POS = 9
	STATE_POS = 10
	ZIP_CODE_POS = 11
	EMPLOYER_POS = 12
	OCCUPATION_POS = 13
	CMTE_ID_POS = 1
	TRAN_DATE_POS = 14
	TRAN_AMOUNT_POS = 15
	TRAN_TYPE_POS = 6
	# ELECTION_CYCLE_POS = 4
	ELECTION_TYPE_POS = 4
	REPORT_TYPE_POS = 3

	# Connect to the database
	conn = connect()
	cur = conn.cursor()
	cur.execute("DROP TABLE IF EXISTS INDIVIDUAL;")
	cur.execute('''CREATE TABLE INDIVIDUAL
	      (NAME 					CHAR(200)  		     	NOT NULL,
	      CITY           			CHAR(30)   				NOT NULL,
	      STATE	 					CHAR(2)     			NOT NULL,
	      ZIP_CODE		    		CHAR(9),
	      EMPLOYER 					CHAR(38),
	      OCCUPATION				CHAR(38),
	      COMMITTEE_ID				CHAR(9),
	      TRAN_DATE					DATE,
	      TRAN_DATE_EXISTS			BOOLEAN,
	      TRAN_AMOUNT 				NUMERIC(14,2),
	      TRAN_TYPE 				CHAR(3),
	      ELECTION_TYPE 			CHAR(1),
	      REPORT_TYPE 				CHAR(3));''')
	print "Table created successfully"
	conn.commit()
	conn.close()

	row_without_date_count = 0 #Count how many of the lines don't have a transaction date
	row_with_date_count = 0 #Count how many of the lines have a transaction date

	for path in paths:
		election_cycle = path[len(path)-6:len(path)-4] # Assuming the path ends in format like ../indiv90.txt
		conn = connect()
		cur = conn.cursor()
		individual = read_files([path])
		individual.insert(0, [])

		for i in range(len(individual[1])):
			NAME = individual[NAME_POS][i]
			CITY = individual[CITY_POS][i]
			STATE = individual[STATE_POS][i]
			ZIP_CODE = individual[ZIP_CODE_POS][i]
			EMPLOYER = individual[EMPLOYER_POS][i]
			OCCUPATION = individual[OCCUPATION_POS][i]
			CMTE_ID = individual[CMTE_ID_POS][i]

			# Seems like the number of rows without date is 0, so this logic is skipped.
			if len(individual[TRAN_DATE_POS][i]) == 0: # If no data or TRAN_DATE skewed, register the election_cycle
				if election_cycle >= '70': # This means the election_cycle is in 1900s
					TRAN_DATE = '19' + election_cycle + '-01-01'
				elif election_cycle < '30': # This means the election_cycle is in 2000s
					TRAN_DATE = '20' + election_cycle + '-01-01'
				row_without_date_count += 1
				TRAN_DATE_EXISTS = "TRUE"
			else:
				TRAN_DATE = str(individual[TRAN_DATE_POS][i][4:]) + '-' + str(individual[TRAN_DATE_POS][i][0:2]) + '-' + str(individual[TRAN_DATE_POS][i][2:4])
				row_with_date_count += 1
				TRAN_DATE_EXISTS = "FALSE"

			TRAN_AMOUNT = individual[TRAN_AMOUNT_POS][i]		
			TRAN_TYPE = individual[TRAN_TYPE_POS][i]
			ELECTION_TYPE = individual[ELECTION_TYPE_POS][i]
			if len(ELECTION_TYPE) > 0:
				ELECTION_TYPE = ELECTION_TYPE[0]
			REPORT_TYPE = individual[REPORT_TYPE_POS][i]
			values_list = [NAME, CITY, STATE, ZIP_CODE, EMPLOYER, OCCUPATION,
				CMTE_ID, TRAN_DATE, TRAN_DATE_EXISTS, TRAN_AMOUNT, TRAN_TYPE, ELECTION_TYPE, REPORT_TYPE]
			query = "INSERT INTO INDIVIDUAL \
				(NAME, CITY, STATE, ZIP_CODE, EMPLOYER, OCCUPATION, COMMITTEE_ID, TRAN_DATE, \
					TRAN_DATE_EXISTS, TRAN_AMOUNT, TRAN_TYPE, ELECTION_TYPE, REPORT_TYPE) \
		      	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
			cur.execute(query, values_list)

			if i%100000 == 0:
				print i
		
		print "Rows without dates: " + str(row_without_date_count)
		print "Rows with dates: " + str(row_with_date_count)

		conn.commit()
		conn.close()
		print "Insertion for the file completed: ", path

	conn.commit()
	conn.close()


def create_committee_candidate_contribution_table(paths):

	'''Create the Candidate Committee Contribution'''
	# Connect to the database
	conn = connect()
	cur = conn.cursor()

	# Positions according to the header file
	CMTE_ID_POS = 1
	CMTE_NAME_POS = 8
	CAND_ID_POS = 17
	# CAND_NAME: Candidate Name // TODO: Add this later from another table?
	TRAN_DATE_POS = 14
	TRAN_AMOUNT_POS = 15
	TRAN_TYPE_POS = 6
	# ELECTION_CYCLE_POS = 4
	ELECTION_TYPE_POS = 4
	# IMAGE_NUM: Do we actually need this?

	cur.execute("DROP TABLE IF EXISTS COMMITTEE_TO_CANDIDATE;")
	cur.execute('''CREATE TABLE COMMITTEE_TO_CANDIDATE
	      (CMTE_ID 					CHAR(9)  		     	NOT NULL,
	      CMTE_NAME           		TEXT    				NOT NULL,
	      CAND_ID	 				CHAR(9)     			NOT NULL,
	      TRAN_DATE		    		DATE,
	      TRAN_AMOUNT				NUMERIC(14,2),
	      TRAN_TYPE					CHAR(3),
	      ELECTION_TYPE				CHAR(1));''')
	print "Table created successfully"
	conn.commit()
	conn.close()

	dateless_count = 0 #Count how many of the lines don't have a transaction date
	for path in paths:	
		conn = connect()
		cur = conn.cursor()
		committee_to_candidate = read_files([path])
		committee_to_candidate.insert(0, [])
		for i in range(len(committee_to_candidate[1])):
			CMTE_ID = committee_to_candidate[CMTE_ID_POS][i]
			CMTE_NAME = committee_to_candidate[CMTE_NAME_POS][i]
			CAND_ID = committee_to_candidate[CAND_ID_POS][i]
			if len(committee_to_candidate[TRAN_DATE_POS][i]) == 0: # In case there is no data
				dateless_count += 1
				print "dateless_counts " + str(dateless_count)
				continue
			TRAN_DATE = str(committee_to_candidate[TRAN_DATE_POS][i][4:]) + '-' + str(committee_to_candidate[TRAN_DATE_POS][i][0:2]) + '-' + str(committee_to_candidate[TRAN_DATE_POS][i][2:4])
			TRAN_AMOUNT = committee_to_candidate[TRAN_AMOUNT_POS][i]
			TRAN_TYPE = committee_to_candidate[TRAN_TYPE_POS][i]
			# ELECTION_CYCLE = committee_to_candidate[ELECTION_CYCLE_POS][i][1:]
			ELECTION_TYPE = committee_to_candidate[ELECTION_TYPE_POS][i]
			if len(ELECTION_TYPE) > 0:
				ELECTION_TYPE = ELECTION_TYPE[0]
			values_list = [CMTE_ID, CMTE_NAME, CAND_ID, TRAN_DATE, TRAN_AMOUNT,
				TRAN_TYPE, ELECTION_TYPE]
			query = "INSERT INTO COMMITTEE_TO_CANDIDATE \
				(CMTE_ID,CMTE_NAME,CAND_ID,TRAN_DATE,TRAN_AMOUNT,TRAN_TYPE,ELECTION_TYPE) \
		      	VALUES (%s, %s, %s, %s, %s, %s, %s)"
			cur.execute(query, values_list)

			if i%100000 == 0:
				print i

		conn.commit()
		conn.close()
		print "Insertion for the file completed: ", path


def create_committee_committee_table(paths):

	CMTE_CONTRIBUTED_ID_POS = 16
	CMTE_CONTRIBUTOR_ID_POS = 1
	CMTE_CONTRIBUTOR_NAME_POS = 8
	TRAN_DATE_POS = 14
	TRAN_AMOUNT_POS = 15
	TRAN_TYPE_POS = 6

	cur.execute("DROP TABLE IF EXISTS COMMITTEE_TO_COMMITTEE;")
	cur.execute('''CREATE TABLE COMMITTEE_TO_COMMITTEE
	      (CMTE_CONTRIBUTED_ID 		CHAR(9)  		     	NOT NULL,
	      CMTE_CONTRIBUTOR_ID       CHAR(9)    				NOT NULL,
	      CMTE_CONTRIBUTOR_NAME	 	TEXT	     			NOT NULL,
	      TRAN_DATE		    		DATE,
	      TRAN_AMOUNT				NUMERIC(14,2),
	      TRAN_TYPE					CHAR(3);''')
	print "Table created successfully"
	conn.commit()
	conn.close()

	dateless_count = 0 #Count how many of the lines don't have a transaction date
	for path in paths:	
		conn = connect()
		cur = conn.cursor()
		committee_to_committee = read_files([path])
		committee_to_committee.insert(0, [])
		for i in range(len(committee_to_committee[1])):
			CMTE_CONTRIBUTED_ID = committee_to_committee[CMTE_CONTRIBUTED_ID][i]
			CMTE_CONTRIBUTOR_ID = committee_to_committee[CMTE_CONTRIBUTOR_ID][i]
			CMTE_CONTRIBUTOR_NAME = committee_to_committee[CMTE_CONTRIBUTOR_NAME][i]
			if len(committee_to_committee[TRAN_DATE_POS][i]) == 0: # In case there is no data
				dateless_count += 1
				print "dateless_counts " + str(dateless_count)
				continue
			TRAN_DATE = str(committee_to_committee[TRAN_DATE_POS][i][4:]) + '-' + str(committee_to_committee[TRAN_DATE_POS][i][0:2]) + '-' + str(committee_to_candidate[TRAN_DATE_POS][i][2:4])
			TRAN_AMOUNT = committee_to_committee[TRAN_AMOUNT_POS][i]
			TRAN_TYPE = committee_to_committee[TRAN_TYPE_POS][i]
			values_list = [CMTE_CONTRIBUTED_ID, CMTE_CONTRIBUTOR_ID, CMTE_CONTRIBUTOR_NAME,
				TRAN_DATE, TRAN_AMOUNT, TRAN_TYPE]
			query = "INSERT INTO COMMITTEE_TO_CANDIDATE \
				(CMTE_CONTRIBUTED_ID,CMTE_CONTRIBUTOR_ID,CMTE_CONTRIBUTOR_NAME,\
					TRAN_DATE,TRAN_AMOUNT,TRAN_TYPE) \
		      	VALUES (%s, %s, %s, %s, %s, %s)"
			cur.execute(query, values_list)

			if i%100000 == 0:
				print i

		conn.commit()
		conn.close()
		print "Insertion for the file completed: ", path


def create_tables():
	# Creating Tables
	# candidate_paths = [(data_dir + 'cn' + str(i)[2:] + '.txt') for i in range(1980, 2020, 2)]
	# create_candidate_table(candidate_paths)

	# committee_paths = [(data_dir + 'cm' + str(i)[2:] + '.txt') for i in range(1980, 2020, 2)]
	# create_committee_table(committee_paths)

	# committee_candidate_paths = [(data_dir + 'pas2' + str(i)[2:] + '.txt') for i in range(1980, 2020, 2)]
	# create_committee_candidate_contribution_table(committee_candidate_paths)

	individual_paths = [(data_dir + 'indiv' + str(i)[2:] + '.txt') for i in range(1996, 2020, 2)]
	create_individual_contribution_table(individual_paths)

	# committee_committee_paths = [(data_dir + 'oth' + str(i)[2:] + '.txt') for i in range(1980, 2020, 2)]
	# create_committee_candidate_contribution_table(committee_committee_paths)

def main():
	# delete_table(connect(), "CANDIDATE")
	create_tables()
	

if __name__ == '__main__':
    main()
