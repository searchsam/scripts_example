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
)

PG_CONN_DEV = psycopg2.connect(
    "host='{host}' dbname='{database}' user='{user}' password='{password}' port='{port}'".format(
        host=os.getenv("PG_HOST_DEV"),
        database=os.getenv("PG_DB_DEV"),
        user=os.getenv("PG_USER_DEV"),
        password=os.getenv("PG_PASS_DEV"),
        port=os.getenv("PG_PORT_DEV"),
    )
)


def primaryKeySequence(table):
    PG_CONN_DEV.cursor().execute(
        "SELECT setval(pg_get_serial_sequence('{table}', 'id'), MAX(id)) FROM {table};".format(
            table=table
        )
    )
    PG_CONN.commit()


def getDf(index):
    MY_CLIENT.execute(getQuery(index))
    return pandas.DataFrame(
        MY_CLIENT.fetchall(), columns=[column[0] for column in MY_CLIENT.description]
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
    PG_CONN.commit()


if __name__ == "__main__":
    tables = {
        0: "users",
        1: "patients",
        2: "coaches",
        3: "sessions",
        4: "examinations",
        5: "appointments",
        6: "memberships",
        7: "body_mass_indices",
        8: "nutritional_plans",
        9: "soaps",
        10: "psychological_plans",
        11: "anxieties",
        12: "blood_pressures",
        13: "electrocardiograms",
        14: "cholesterol_levels",
        15: "depressions",
        16: "glucose_levels",
        17: "hemoglobins",
        18: "spirometries",
        19: "triglyceride_levels",
        20: "dreams",
        21: "goals",
        22: "diagnostics",
        23: "habits",
        24: "controls",
        25: "questions",
        26: "items",
        27: "kiosks",
        28: "stocks",
        29: "events",
        30: "employees",
        31: "employees_kiosks",
        32: "sales",
        33: "items_sales",
    }

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

        # Crear usuario doctor para Luz
        # INSERT INTO users(area_code, birthday, email, encrypted_password, passwd_login, role, created_at, updated_at) VALUES ('+505', '1995-02-09', 'luz@estacionvital.com', '$2a$10$7bRq5mXRAkC3TrrjGLknGOHOHM7e60eoeiPUXroguNXLFehLgshzS', TRUE, 3, NOW(), NOW());
        # INSERT INTO coaches (available, last_name, last_seen_at, name, sex, user_id, created_at, updated_at) VALUES (TRUE, 'Luz', '2021-03-17 11:12:10', 'Coach', TRUE, 73356, NOW(), NOW());
