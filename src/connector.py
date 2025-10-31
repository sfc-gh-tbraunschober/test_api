import datetime
import os

import snowflake.connector
from snowflake.connector import DictCursor
from flask import Blueprint, request, abort, jsonify, make_response

# Make the Snowflake connection

def connect() -> snowflake.connector.SnowflakeConnection:
    if os.path.isfile("/snowflake/session/token"):
        creds = {
            'host': os.getenv('SNOWFLAKE_HOST'),
            'port': os.getenv('SNOWFLAKE_PORT'),
            'protocol': "https",
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'authenticator': "oauth",
            'token': open('/snowflake/session/token', 'r').read(),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'client_session_keep_alive': True
        }
    else:
        creds = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'client_session_keep_alive': True
        }
    return snowflake.connector.connect(**creds)

conn = connect()

# Make the API endpoints
connector = Blueprint('connector', __name__)

## Top 10 customers in date range
dateformat = '%Y-%m-%d'

@connector.route('/tab/last10')
def customers_top10():
    try:
        None
    except:
        abort(400, "Invalid start and/or end dates.")
    sql_string = '''
        SELECT
            mykey, myval
        FROM apidb.apis.tab
        ORDER BY mykey DESC
        LIMIT 10
    '''
    sql = sql_string
    try:
        res = conn.cursor(DictCursor).execute(sql)
        return make_response(jsonify(res.fetchall()))
    except:
        abort(500, "Error reading from Snowflake. Check the logs for details.")

## Monthly sales for a clerk in a year
@connector.route('/area/<myarea>/key/<mykey>')
def clerk_montly_sales(myarea, mykey):
    # Validate arguments
    if not myarea.isdigit():
        abort(400, "Area can only contain numbers.")
    if not mykey.isdigit():
        abort(400, "Key can only contain numbers.")

    sql_string = '''
        SELECT
            mykey, myval, myarea
        FROM apidb.apis.tab
        WHERE mykey = {mykey}
          AND myarea = {myarea}
        ORDER BY myarea, mykey
    '''
    sql = sql_string.format(mykey=int(mykey), myarea=int(myarea))
    try:
        res = conn.cursor(DictCursor).execute(sql)
        return make_response(jsonify(res.fetchall()))
    except:
        abort(500, "Error reading from Snowflake. Check the logs for details.")
