#!/bin/bash python3
import pymysql
import json
import requests
import os
import gzip


"""
This script is used to identify missing and incomplete system data in the Canonn database.
Initially it loads missing data from spansh into the star_systems and system_bodies tables.

Once all missing systems are loaded it will load all systems where there is system and body 
data present but the data is incomplete.

If the data fetched from spansh for the systems has not changed since it was last loaded in
the database then the script will not attempt to update the database. 

Roughly 11% of the 500,000+ systems are incomplete and many will never be fully updated. 
This means that the second phase of the script  will spend most of its time querying spansh
without updating. 

Around 800 new systems are added each day. 

"""

mysql_conn = None
id64_dict = {}


class SSDictCursor(pymysql.cursors.SSCursor, pymysql.cursors.DictCursorMixin):
    """
    A cursor that uses server-side cursors and returns rows as dictionaries.
    """

    pass


def connect_database(database_secrets_file):
    global mysql_conn  # Declare `mysql_conn` as a global variable
    print("Database Secrets File:", database_secrets_file)

    # Open the JSON file containing database secrets
    with open(database_secrets_file) as json_file:
        secret = json.load(json_file)  # Load the JSON data into a dictionary

        # Establish a connection to the MySQL database
        mysql_conn = pymysql.connect(
            host=secret.get("DB_HOST"),  # Get the host from secrets
            user=secret.get("DB_USER"),  # Get the username from secrets
            password=secret.get("DB_PASSWORD"),  # Get the password from secrets
            db=secret.get("DB_NAME"),  # Get the database name from secrets
            port=int(
                secret.get("DB_PORT", 3306)
            ),  # Get the port from secrets, default to 3306 if not provided
            charset="utf8",  # Set the character set to UTF-8,
            # cursorclass=SSDictCursor,
            cursorclass=pymysql.cursors.DictCursor,
        )
        return mysql_conn


def count_bodies(bodies):
    bodyCount = 0
    for body in bodies:
        if body.get("type") in ["Planet", "Star"]:
            bodyCount += 1
    return bodyCount


def fetch_systems(rows, complete):
    session = requests.Session()
    headers = {"User-Agent": "Canonn firmament.py"}
    systems = []

    for row in rows:

        if complete or id64_dict.get(row.get("id64")):

            url = f"https://spansh.co.uk/api/dump/{row.get('id64')}"
            response = session.get(url, headers=headers)

            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                data = response.json().get("system")

                # if its not complete then we can check for mismatch
                data["lenBodies"] = count_bodies(data.get("bodies"))
                if not complete:
                    changed = not (
                        data["lenBodies"] == row.get("len_bodies")
                        and (data.get("bodyCount") or 0) == row.get("body_count")
                    )

                if complete or not changed:
                    print(f"No change {data.get('name')}")
                    systems.append(data)
                else:
                    print(f"Fetched for update {data.get('name')}")
            else:
                print(f"Missing {row.get('name')} ({row.get('id64')})")
        # else:
        #    print(f"Skipping {row.get('name')} ({row.get('id64')})")

    session.close()
    return systems


missing_systems_query = """
            SELECT DISTINCT CAST(id64 AS CHAR) AS id64, `system` as name FROM codexreport cr where not exists
            (select 1 from star_systems ss where ss.id64 = cr.id64)
"""

incomplete_systems_query = """
          SELECT DISTINCT CAST(cr.id64 AS CHAR) AS id64, cr.`system` as name,ifnull(ss.body_count,0) as body_count, ss.len_bodies  FROM codexreport cr
          join star_systems ss on ss.id64 = cr.id64
          where ifnull(ss.bodies_match,0) != 1
"""


def insert_systems(systems):
    cursor = mysql_conn.cursor()
    cursor.executemany(
        "INSERT IGNORE INTO star_systems (raw_json) VALUES (%s) ON DUPLICATE KEY UPDATE raw_json = VALUES(raw_json)",
        (systems),
    )
    print(f"{cursor.rowcount} systems out of {len(systems)} inserted.")

    cursor.close()


def insert_bodies(bodies):
    cursor = mysql_conn.cursor()
    cursor.executemany(
        "INSERT IGNORE INTO system_bodies (raw_json) VALUES (%s) ON DUPLICATE KEY UPDATE raw_json = VALUES(raw_json)",
        (bodies),
    )
    print(f"{cursor.rowcount} bodies out of {len(bodies)} inserted.")

    cursor.close()


def process(query, complete=True):
    global mysql_conn

    print("Processing query:", query)

    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(
        query,
    )
    print("Query executed.")
    while True:
        system_values = []
        body_values = []
        rows = cursor.fetchmany(200)

        if not rows:
            break

        print(f"Fetching {len(rows)} systems...")
        system_data = fetch_systems(rows, complete)
        print(f"{len(system_data)} systems for insert.")

        for data in system_data:

            if data:
                bodies = data.pop("bodies", [])

                for body in bodies:
                    body["systemAddress"] = data.get("id64")
                    body_json = json.dumps(body)
                    body_values.append(body_json)

                system_values.append(json.dumps(data))
        if len(system_values) > 0:
            insert_systems(system_values)
        if len(body_values) > 0:
            insert_bodies(body_values)
            mysql_conn.commit()
    # close the cursor we are done
    cursor.close()


def download_and_process_json():
    # Download the file
    response = requests.get("https://downloads.spansh.co.uk/systems_1week.json.gz")
    response.raise_for_status()  # Ensure the request was successful

    # Decompress the gzip file
    decompressed_data = gzip.decompress(response.content)

    # Load the JSON data
    data = json.loads(decompressed_data)

    # Process the JSON data to create a dictionary keyed on 'id64'
    result_dict = {}
    for item in data:
        id64 = item["id64"]
        result_dict[id64] = True

    return result_dict


def main():
    global id64_dict
    # create the path $HOME/.ssh/database_secrets.json using
    file_location = os.path.join(os.environ["HOME"], ".ssh", "database_secrets.json")
    mysql_conn = connect_database(file_location)

    # first we are going to process all the systems that are not in the database
    # then we will work on systems that are out of date

    process(missing_systems_query)
    id64_dict = download_and_process_json()

    process(incomplete_systems_query, complete=False)


main()
