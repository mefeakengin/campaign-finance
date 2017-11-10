import os
import json

import bills.setup
import lda.setup
import matching.setup
import firms.setup
import analytics.setup
import tariffs.setup


def do(entity, action):
    print "[%s] %s - start"%(entity, action)
    if entity == "database":
        if action == "clear":
            clear()
        elif action == "reset":
            reset()
    elif entity == "bills":
        getattr(bills.setup, action)()
    elif entity == "reports":
        getattr(lda.setup, action)()
    print "[%s] %s - end"%(entity, action)

def clear():
    import sqlalchemy

    engine = sqlalchemy.create_engine("postgres://postgres:postgres@localhost/postgres")
    conn = engine.connect()
    conn.execute("commit")
    conn.execute("select pg_terminate_backend (pg_stat_activity.pid) from pg_stat_activity where pg_stat_activity.datname = 'lobby'")
    conn.execute("drop database if exists lobby")
    conn.execute("commit")
    conn.execute("create database lobby")
    conn.close()


def reset():
    import sqlalchemy

    engine = sqlalchemy.create_engine("postgres://postgres:postgres@localhost/postgres")
    conn = engine.connect()
    conn.execute("commit")
    conn.execute("select pg_terminate_backend (pg_stat_activity.pid) from pg_stat_activity where pg_stat_activity.datname = 'lobby'")
