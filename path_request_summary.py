# -*-coding:utf-8-*-

import MySQLdb
import sys

SQL_GET_ALL_RECORDS = "select request_count from path_request_count where path_id not in(0, -1) order by request_count desc;"
SQL_INSERT_SUM = "insert into path_request_summary (path_count, request_sum) values (%s, %s);"

DB = "test_db"
HOST = "127.0.0.1"
PORT = 33333
USER = "root"
PASSWORD = "root"


def main():
    print("START")
    summary()
    print("END")


def summary():

    request_sum = 0
    record_count = 0
    try:
        connection = getConnection()
        for record in get_all_record(SQL_GET_ALL_RECORDS, connection):
            request_sum += record[0]
            print(request_sum)
            record_count += 1
            insert_request_sum_with_connection(record_count, request_sum, connection)
    except:
        print("[SQL:!!!FAILED!!!]" + SQL_INSERT_SUM)
        sys.exit(1)
    finally:
        connection.close()


def get_all_record(sql, my_connection):
    return execute_select_with_connection(sql, my_connection)


def insert_request_sum_with_connection(record, sum, my_connection):
    try:
        cursor = my_connection.cursor()
        cursor.execute(SQL_INSERT_SUM, (record, sum))
    except:
        print("[SQL:!!!FAILED!!!]" + SQL_INSERT_SUM)
        sys.exit(1)
    finally:
        my_connection.commit()


def execute_select_with_connection(sql_select, my_connection):
    try:
        cursor = my_connection.cursor()
        cursor.execute(sql_select)
        result = cursor.fetchall()
    except:
        result = None
        sys.exit(1)
    return result


def getConnection():
    try:
        connection = MySQLdb.connect(
            host=HOST, user=USER, port=PORT, passwd=PASSWORD, db=DB, charset='utf8')
    except:
        print("HOST:" + HOST + " connetction error.")
        sys.exit(1)
    return connection


if __name__ == '__main__':
    main()
