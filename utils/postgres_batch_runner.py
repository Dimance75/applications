import argparse, ConfigParser, os
import psycopg2

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Run SQL Scripts on DWH.')
    parser.add_argument('config_file_name', help='Absolute path to config-file')
    parser.add_argument('sql_folder_name', help='Absolute path to the SQL folder.')
    args = parser.parse_args()

    config = ConfigParser.ConfigParser()
    config.read(args.config_file_name)

    section = 'dwh'
    db_creds = dict(config.items(section))
    print(db_creds)

    db_name = db_creds['schema']
    db_user = db_creds['user']
    db_pw = db_creds['pass']
    db_host = db_creds['host']

    sql_list = []

    for filename in os.listdir(args.sql_folder_name):
        if filename.endswith(".sql"): sql_list.append(filename)

    sql_list_sorted = sorted(sql_list)

    with psycopg2.connect(database=db_name, user=db_user, password=db_pw, host=db_host) as connection_var:
        print("Successfully established DB connection.")
        with connection_var.cursor() as cur:
            print("Successfully created DB cursor.")
            for filename in sql_list_sorted:
                with open(args.sql_folder_name + filename, 'r') as q:
                    cur.execute(q.read())
                    print('Query successfully ran: %s') % filename
