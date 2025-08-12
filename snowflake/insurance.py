import datetime
import os
import valohai
from snowflake.snowpark import Session

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

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
        session = Session.builder.configs({
            "account": SNOWFLAKE_ACCOUNT,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA,
            "host": SNOWFLAKE_HOST,
            "token": get_login_token(),
            "authenticator": "oauth",
            "role": SNOWFLAKE_ROLE,
        }).create()
        insurance_table = session.table("insurance.ml_pipe.source_of_truth")
        insurance_rows = insurance_table.collect()
        print("Insurance Table Data has %d rows:", len(insurance_rows))
        timestamp = datetime.datetime.now().isoformat()
        filename = f"insurance_data_{timestamp}.csv"
        output = valohai.outputs().path(filename)
        print(f"Writing data to {output}")
        insurance_table.to_df().to_csv(output, index=False)
        
        
    except Exception as e:
        print(f"Error creating Snowflake session: {e}")
    
    if session:
        session.close()