import psycopg2
import os
import valohai
import json
import time


def connect():
    conn_str = os.environ.get("PG_CONN_STR")
    conn = psycopg2.connect(conn_str)

    query_sql = valohai.parameters("sql_query").value

    cur = conn.cursor()
    cur.execute(query_sql)

    version = cur.fetchone()[0]
    print(version)
    output = valohai.outputs().path("subdir/output.txt")
    with open(output, "w") as file:
        file.write(version)
        file.write("\n")
        file.write(str(time.time()))

    metadata = {"valohai.alias": valohai.parameters("datum_alias").value}
    metadata_path = valohai.outputs().path(f"{output}.metadata.json")
    with open(metadata_path, "w") as metadata_out:
        json.dump(metadata, metadata_out)


if __name__ == "__main__":
    connect()
