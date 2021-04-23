#!/usr/bin/.venv python

import os
import pandas
import pathlib
import psycopg2
import pymysql.cursors
import subprocess
import time
from dotenv import load_dotenv

load_dotenv()

PG_CONN = psycopg2.connect(
    "host='{host}' dbname='{database}' user='{user}' password='{password}' port='{port}'".format(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASS"),
        port=os.getenv("PG_PORT"),
    )
).cursor()

MA_CLIENT = pymysql.connect(
    host=os.getenv("MA_HOST"),
    database=os.getenv("MA_DB"),
    user=os.getenv("MA_USER"),
    password=os.getenv("MA_PASS"),
    cursorclass=pymysql.cursors.DictCursor,
).cursor()


def makeRun():
    MA_CLIENT.execute(
        "INSERT INTO runs (run_time, created_at, updated_at) VALUES ({time}, NOW(), NOW());".format(
            time=int(time.time_ns() / 1e9)
        )
    )
    return MA_CLIENT.lastrowid


def countRecords(table, run_id):
    last_id = 0

    PG_CONN.execute(
        "SELECT id FROM {table} ORDER BY id DESC LIMIT 1;".format(table=table)
    )
    temp_item = PG_CONN.fetchone()
    if temp_item is None:
        last_id = 0
    else:
        last_id = temp_item[0]

    PG_CONN.execute("SELECT count(*) FROM {table};".format(table=table))
    record_count = PG_CONN.fetchone()[0]

    MA_CLIENT.execute(
        "INSERT INTO records (column_name, last_id, record_count, run_id, created_at, updated_at) VALUES ('{table}', {last_id}, {record_count}, {run_id}, NOW(), NOW());".format(
            table=table, last_id=last_id, record_count=record_count, run_id=run_id
        )
    )


if __name__ == "__main__":
    tables = [
        "anxieties",
        "appointments",
        "blood_pressures",
        "body_mass_indices",
        "cholesterol_levels",
        "coaches",
        "controls",
        "depressions",
        "diagnostics",
        "dreams",
        "electrocardiograms",
        "employees",
        "employees_kiosks",
        "examinations",
        "glucose_levels",
        "goals",
        "habits",
        "hemoglobins",
        "items",
        "items_sales",
        "kiosks",
        "memberships",
        "nutritional_plans",
        "patients",
        "psychological_plans",
        "questions",
        "sales",
        "sessions",
        "soaps",
        "spirometries",
        "stocks",
        "triglyceride_levels",
        "users",
    ]

    run_id = makeRun()

    for item in tables:
        print("Contando tabla {}".format(item))
        countRecords(item, run_id)
