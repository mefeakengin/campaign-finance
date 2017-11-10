import logging
import os
import string

from elixir import *
from path import path
import os

from database.lda.models import *
from database.bills.models import *
from database.tariffs.models import *
from database.firms.models import *

# Testing mode --- incomplete feature, don't set this
TEST_MODE = False

# Range of congresses under consideration
CONGRESSES = range(106, 115+1)
CONGRESSES_PEOPLE = range(106, 115+1)
CONGRESSES_TO_YEAR = range(1999, 2017+1)
# 114th congress info does not exist yet: check with the following
# rsync -avz --delete --delete-excluded govtrack.us::govtrackdata/us/114/people.xml ./

# Important directories and files
# OUTPUT_DIR = path('./output')
# ROOT_DIR = path('./database')
# DATA_DIR = path('/tigress/dkunisky/pol/')
DATA_DIR = path(__file__).parent.parent.parent / 'data'
# LOBBY_REPO_DIR = path('../matching')
# CLIENT_NAME_MATCHES_FILE = DATA_DIR / 'client_naming/matches.csv'

# if not OUTPUT_DIR.exists():
#     os.makedirs(OUTPUT_DIR)
# if not ROOT_DIR.exists():
#     os.makedirs(ROOT_DIR)
# if not DATA_DIR.exists():
#     os.makedirs(DATA_DIR)

# DATABASE_DIR = DATA_DIR / 'database'
# DATABASE_DIR = DATA_DIR / 'database_backup'

# if not DATABASE_DIR.exists():
#     os.makedirs(DATABASE_DIR)

# TESTS_DIR = ROOT_DIR / 'tests'

# if TEST_MODE:
#     DATABASE_FILE = TESTS_DIR / 'test.db'
# else:
#     DATABASE_FILE = DATABASE_DIR / 'main.db'


def init_db():
    metadata.bind = "postgresql://postgres:P)STGR#S@localhost:5432/lobby"
    metadata.bind.dialect.max_identifier_length = 256

    setup_all()
    create_all()

def flush_db():
    session.flush()

def close_db(write=False):
    if write:
        session.commit()
    else:
        session.rollback()

def clean_string(s):
    r = string.replace(s, "\n", " ")
    r = string.replace(r, "\r", " ")
    r = string.replace(r, "\t", " ")

    return r
