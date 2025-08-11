from snowflake.snowpark import Session

def check_connection():
    session = Session.builder.getOrCreate()
    database = session.get_current_database()
    schema = session.get_current_schema()
    print(f"Connected to database: {database}, schema: {schema}")
    session.close()

if __name__ == "__main__":
    check_connection()