#!/usr/bin/.venv python

import os
import pandas
import pathlib
import psycopg2
import subprocess
from dotenv import load_dotenv

load_dotenv()

PG_CONN_PROD = psycopg2.connect(
    "host='{host}' dbname='{database}' user='{user}' password='{password}' port='{port}'".format(
        host=os.getenv("PG_HOST_PROD"),
        database=os.getenv("PG_DB_PROD"),
        user=os.getenv("PG_USER_PROD"),
        password=os.getenv("PG_PASS_PROD"),
        port=os.getenv("PG_PORT_PROD"),
    )
).cursor()

PG_CONN_DEV = psycopg2.connect(
    "host='{host}' dbname='{database}' user='{user}' password='{password}' port='{port}'".format(
        host=os.getenv("PG_HOST_DEV"),
        database=os.getenv("PG_DB_DEV"),
        user=os.getenv("PG_USER_DEV"),
        password=os.getenv("PG_PASS_DEV"),
        port=os.getenv("PG_PORT_DEV"),
    )
)


def getQuery(queryIndex):
    return {
        0: "SELECT users.id, users.area_code, users.birthday, users.email, users.encrypted_password, users.firebase_id, CASE WHEN patients.sex IS TRUE THEN 1 ELSE 2 END as gender, patients.last_name, patients.name, users.passwd_login, users.phone, patients.stripe_id, patients.stripe_subscription_id, users.role as role_id, users.created_at, users.updated_at FROM users, patients WHERE patients.user_id = users.id ORDER BY users.created_at ASC;",
        1: "SELECT users.id, users.area_code, coaches.available, users.birthday, users.email, users.encrypted_password, users.firebase_id, CASE WHEN coaches.sex IS TRUE THEN 1 ELSE 2 END as gender, coaches.last_name, coaches.name, users.passwd_login, users.phone, users.role as role_id, users.created_at, users.updated_at FROM users, coaches WHERE coaches.user_id = users.id ORDER BY users.created_at ASC;",
    }[queryIndex]


def getDf(index):
    PG_CONN_PROD.execute(getQuery(index))
    return pandas.DataFrame(
        PG_CONN_PROD.fetchall(),
        columns=[column[0] for column in PG_CONN_PROD.description],
    )


def copyToDB(table, cols):
    command = """ psql -h {host} -d {database} -U {user} -p {port} -t -A -c \"\COPY {table} ({columns})  from '/tmp/{table}.csv' DELIMITER ',' CSV HEADER; \" """.format(
        host=os.getenv("PG_HOST_DEV"),
        database=os.getenv("PG_DB_DEV"),
        user=os.getenv("PG_USER_DEV"),
        port=os.getenv("PG_PORT_DEV"),
        columns=cols,
        table=table,
    )
    os.putenv("PGPASSWORD", os.getenv("PG_PASS_DEV"))
    active = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE
    ).stdout.readlines()
    print(active[0].decode().strip())


def primaryKeySequence(table):
    PG_CONN_DEV.cursor().execute(
        "SELECT setval(pg_get_serial_sequence('{table}', 'id'), MAX(id)) FROM {table};".format(
            table=table
        )
    )
    PG_CONN_DEV.commit()


if __name__ == "__main__":
    tables = {0: "users", 1: "users", 2: "devices"}

    for index, table in tables.items():
        try:
            print(table)
            df = getDf(index)
            df.to_csv(
                "/tmp/{}.csv".format(table), header=True, index=False, encoding="utf-8"
            )
            cols = ",".join(list(df.columns))
            copyToDB(table, cols)
            primaryKeySequence(table)
            os.remove("/tmp/{}.csv".format(table))
        except Exception as e:
            print(e)
            if pathlib.Path("/tmp/{}.csv".format(table)).exists():
                os.remove("/tmp/{}.csv".format(table))

            exit()
