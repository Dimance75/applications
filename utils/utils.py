# -*- coding: utf-8 -*-
import os
import smtplib
import traceback
import chardet
import re
import configparser

from os.path import basename
from oauth2client.service_account import ServiceAccountCredentials
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

CLIENT_SECRET_FILE = 'sa_credentials.json'
SCOPES = 'https://spreadsheets.google.com/feeds'
CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'config/', 'global_config.ini')


def send_email(user, pwd, recipient, cc, subject, body, files=None):
    """
    send email via googlemail
    :param user:
    :param pwd:
    :param recipient:
    :param cc:
    :param subject:
    :param body:
    :param files:
    :return:
    """
    # Prepare actual message
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = user
    msg['To'] = ','.join(recipient)
    msg['Cc'] = ','.join(cc)
    msg.add_header('Content-Type', 'text/html')
    msg.attach(MIMEText(body, 'html'))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
            msg.attach(part)

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.ehlo()
        server.starttls()
        server.login(user, pwd)
        server.sendmail(msg['From'], recipient + cc, msg.as_string())
        server.close()
        print 'successfully sent the mail'
    except:

        print "failed to send mail"
        print traceback.print_exc()


def generate_email_message(data, column_names, title='', description=''):
    msg = """
            <!DOCTYPE html>
            <html>
            <head>
            <title>HTML Tables</title>
            </head>
            <body>
            <table border="1">
            """
    msg += '<tr><td colspan=' + str(len(data) + 1) + '>' + title

    msg_data = [[column.replace('_', ' ').title(), ] for column in column_names]
    for row in data:
        for index, item in enumerate(row):
            msg_data[index].append(item)

    for items in msg_data:
        msg += '<tr>'
        for item in items:
            msg += '<td style="text-align:right">' + str(item) + '</td>'
        msg += '</tr>'
    msg += """
            </table>

            {desc}
            <br><br>
            Mit freundlichen Grüßen / Best Regards,<br>

            </body>
            </html>
            """.format(desc=description.replace('\n', '<br>'))
    return msg


def get_credentials():
    """
    get creentials for google sheets access
    :return:
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   CLIENT_SECRET_FILE)
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_path, scopes=SCOPES)
    return credentials


def perdelta(start, end, delta):
    """
    :param start: date, datetime
    :param end: date,datetime
    :param delta: timedelta step size
    :return: generator over the dates/datetimes
    """
    curr = start
    while curr <= end:
        yield curr
        curr += delta


def db_create_insert_query(cursor, dest, column_names, row):
    fields = ', '.join(column_names)
    values = ', '.join(['%%(%s)s' % x for x in column_names])
    query = 'INSERT INTO %s (%s) VALUES (%s);' % (dest, fields, values)
    return cursor.mogrify(query, row)


def camel_case_to_snake_case(string):
    """
    Converts CamelCase string into snake_case string
    :param string:
    :return:
    """
    step1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', step1).lower()

def get_global_config(config_path=CONFIG_PATH):
    """
    Returns global config. Uses default path ('USER_HOME/config/global_config.ini') if used without parameter.
    :param config_path: Path to the config file.
    :return: ConfigParser instance
    """
    config_parser = configparser.ConfigParser()
    config_parser.read(config_path)
    return config_parser