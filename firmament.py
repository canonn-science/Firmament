import pymysql
import json
import requests
import os

mysql_conn = None


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
            cursorclass=pymysql.cursors.DictCursor,
        )
        return mysql_conn


def count_bodies(bodies):
    bodyCount = 0
    for body in bodies:
        if body.get("type") in ["Planet", "Star"]:
            bodyCount += 1
    return bodyCount


def system_current(id64):
    cursor = mysql_conn.cursor()
    cursor.execute(
        "SELECT bodies_match FROM star_systems WHERE id64 = %s and bodies_match = 1",
        (id64,),
    )
    system = cursor.fetchmany()
    cursor.close()

    return cursor.rowcount


def fetch_system(id64):
    url = f"https://spansh.co.uk/api/dump/{id64}"
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json().get("system")
        return data


def load_system(id64):

    if system_current(id64) > 0:
        print("System already loaded.")
        return

    data = fetch_system(id64)

    # Check if the request was successful
    if data:

        # Extract the 'bodies' key from the data
        bodies = data.pop("bodies", [])
        # now we can replace with the count of bodies
        data["lenBodies"] = count_bodies(bodies)
        # Prepare JSON strings for insertion
        star_system_json = json.dumps(data)

        # Establish the database connection
        cursor = mysql_conn.cursor()

        # Insert the star system JSON
        cursor.execute(
            "INSERT IGNORE INTO star_systems (raw_json) VALUES (%s) ON DUPLICATE KEY UPDATE raw_json = VALUES(raw_json);",
            (star_system_json,),
        )
        if cursor.rowcount == 0:
            print(f"{data.get('name')} Already populated in database.")

        # Insert each body JSON
        values = []
        for body in bodies:
            body["systemAddress"] = data.get("id64")
            body_json = json.dumps(body)
            values.append(body_json)

        cursor.executemany(
            "INSERT IGNORE INTO system_bodies (raw_json) VALUES (%s) ON DUPLICATE KEY UPDATE raw_json = VALUES(raw_json);",
            (values),
        )
        print(f"{cursor.rowcount} bodies out of {len(values)} inserted.")
        cursor.close()
        mysql_conn.commit()
        print(
            f"System {data.get('name')} and {data.get('lenBodies')} bodies loaded successfully."
        )
    else:
        print("Failed to load system.")


# create the path $HOME/.ssh/database_secrets.json using
file_location = os.path.join(os.environ["HOME"], ".ssh", "database_secrets.json")

mysql_conn = connect_database(file_location)

with mysql_conn.cursor() as cursor:
    # Execute the query
    cursor.execute(
        """
        SELECT DISTINCT id64 FROM codexreport cr where not exists
        (select 1 from star_systems ss where ss.id64 = cr.id64 and ss.bodies_match = 1)
    """
    )

    # Fetch all the distinct id64 values
    # Fetch one row at a time
    row = cursor.fetchone()
    while row is not None:
        load_system(row.get("id64"))
        row = cursor.fetchone()
