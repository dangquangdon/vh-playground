import psycopg2
import os
import valohai


def connect():
    conn_str = os.environ.get("PG_CONN_STR")
    conn = psycopg2.connect(conn_str)

    query_sql = 'SELECT VERSION()'

    cur = conn.cursor()
    cur.execute(query_sql)

    version = cur.fetchone()[0]
    print(version)
    output = valohai.outputs().path("output.txt")
    with open(output, "w") as file:
        file.write(version)

if __name__ == '__main__':
    connect()