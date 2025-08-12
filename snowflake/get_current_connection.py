import os
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from snowflake.connector import connect


def get_login_token():
    try:
        with open("/snowflake/session/token", "r") as f:
            return f.read()
    except Exception as e:
        print(f"Error reading token file: {e}")
        return None


if __name__ == "__main__":
    session = None
    try:
        session = get_active_session()
        if session:
            database = session.get_current_database()
            schema = session.get_current_schema()
            print(f"Connected to database: {database}, schema: {schema}")
    except Exception as e:
        print(f"Error getting active session: {e}")

    if not session:
        print("No active Snowflake session found.")
        print("Checking environment variables for connection parameters.")

        print("SNOWFLAKE_ACCOUNT", os.getenv("SNOWFLAKE_ACCOUNT"))
        print("SNOWFLAKE_USER", os.getenv("SNOWFLAKE_USER"))
        print("SNOWFLAKE_ROLE", os.getenv("SNOWFLAKE_ROLE"))
        print("SNOWFLAKE_WAREHOUSE", os.getenv("SNOWFLAKE_WAREHOUSE"))
        print("SNOWFLAKE_DATABASE", os.getenv("SNOWFLAKE_DATABASE"))
        print("SNOWFLAKE_SCHEMA", os.getenv("SNOWFLAKE_SCHEMA"))
        print("SNOWFLAKE_HOST", os.getenv("SNOWFLAKE_HOST"))
        print("TOKEN", get_login_token())
        creds = {
            key: value
            for key, value in {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "role": os.getenv("SNOWFLAKE_ROLE"),
                "database": os.getenv("SNOWFLAKE_DATABASE"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "host": os.getenv("SNOWFLAKE_HOST"),
                "token": get_login_token(),
                "authenticator": "oauth",
            }.items()
            if value is not None
        }

        print("Connecting with credentials:", creds)
        try:
            
            session = Session.builder.configs(creds).create()
            database = session.get_current_database()
            schema = session.get_current_schema()
            print(f"Connected to database session: {database}, schema: {schema}")
            session.close()
        except Exception as e:
            print(f"Error creating session: {e}")
            try:
                conn = connect(**creds)
                print("Connected to Snowflake using connector.")
                conn.close()
            except Exception as e:
                print(f"Error connecting with connector: {e}")
                print("Failed to connect to Snowflake.")
