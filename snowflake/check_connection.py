import os
from pprint import pprint
from snowflake.snowpark import Session


def check_connection():
    params = {
        "account": os.getenv("VH_ENV_SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("VH_ENV_SNOWFLAKE_USER"),
        "role": os.getenv("VH_ENV_SNOWFLAKE_ROLE"),
        "database": os.getenv("VH_ENV_SNOWFLAKE_DATABASE"),
        "schema": os.getenv("VH_ENV_SNOWFLAKE_SCHEMA"),
        "warehouse": os.getenv("VH_ENV_SNOWFLAKE_WAREHOUSE"),
        "token": os.getenv("VH_ENV_SNOWFLAKE_JWT_TOKEN"),
        "host": os.getenv("VH_ENV_SNOWFLAKE_HOST"),
        "authenticator": "oauth",
    }
    pprint(params)
    session = Session.builder.configs(params).create()
    database = session.get_current_database()
    schema = session.get_current_schema()
    print(f"Connected to database: {database}, schema: {schema}")
    session.close()


if __name__ == "__main__":
    check_connection()
