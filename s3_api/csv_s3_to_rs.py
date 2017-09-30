import csv
import psycopg2

def csv_s3_to_rs(csv_full_path_s3, s3_key, s3_secret, rs_name, rs_port, rs_user, rs_pwd, rs_host, rs_table):
    try:

        # Connect to RedShift
        conn_string = "dbname=%s port=%s user=%s password=%s host=%s" % (rs_name, rs_port, rs_user, rs_pwd, rs_host)
        conn = psycopg2.connect(conn_string)
        print("Connection to Redshift successful.")

        # Create the cursor object.
        cursor = conn.cursor()

        copy_query = """
        COPY %s
        from '%s'
        CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'
        DELIMITER AS ','
        ;
        """ % (rs_table, csv_full_path_s3, s3_key, s3_secret)

        # Execute this COPY the file stored in S3 bucket to Redshift.
        print("Running COPY query...")
        cursor.execute(copy_query)
        print("COPY query worked successfully.")

    except Exception as inst:
        print(inst)
        conn.rollback()
        print("Error when copying CSV data to Redshift, rolled back changes.")

    finally:
        conn.commit()
        conn.close()
        print("Connection to Redshift closed.")