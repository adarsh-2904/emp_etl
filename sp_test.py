import psycopg2
from psycopg2 import sql, OperationalError
from datetime import datetime

def call_stored_procedure():
    conn_info = {
        'host': 'redshift.test.datahub.redcross.net',
        'port': 5439,
        'dbname': 'mods_bi',
        'user': 'adarsh_ram',
        'password': '3c7liI8myEkEKJUZe4JB'
    }

    try:
        conn = psycopg2.connect(**conn_info)
        conn.autocommit = True
        cur = conn.cursor()

        # Call the stored procedure
        print(f"Calling stored procedure at {datetime.now()}")
        cur.execute(sql.SQL("CALL mktg_ops_tbls.ld_test_sp();"))  

    except OperationalError as e:
        print("Database connection failed:", e)

    except Exception as e:
        print("Error while executing stored procedure:", e)

    else:
        # Only print once here if everything ran successfully
        print(f"Stored procedure completed successfully at {datetime.now()}")

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    call_stored_procedure()
