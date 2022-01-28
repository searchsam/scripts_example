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


if __name__ == "__main__":
    ids = [62, 63, 64, 65]
    days = [
        "friday",
        "monday",
        "saturday",
        "sunday",
        "thursday",
        "tuesday",
        "wednesday",
    ]

    for id in ids:
        print(id)

        for day in days:
            print(day)
            PG_CONN_PROD.execute(
                "SELECT {day}_breakfast as breakfast, '{day}' as day, {day}_dinner as dinner, {day}_lunch as lunch, {day}_snack as snack, {day}_snack_dinner as snack_dinner, {day}_snack_lunch as snack_lunch FROM nutritional_plans WHERE id = {id}".format(
                    day=day, id=id
                )
            )
            r = PG_CONN_PROD.fetchall()
            print(r[0])

            PG_CONN_DEV.cursor().execute(
                "INSERT INTO plan_days (breakfast, day, dinner, lunch, snack, snack_dinner, snack_lunch) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                r[0],
            )
            PG_CONN_DEV.commit()

            PG_CONN_DEV.cursor().execute(
                "INSERT INTO nutritional_plans_plan_days (nutritional_plan_id, plan_day_id) VALUES ({id}, (SELECT id FROM plan_days ORDER BY id DESC LIMIT 1))".format(
                    id=id
                )
            )
            PG_CONN_DEV.commit()
