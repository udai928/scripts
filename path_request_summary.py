# -*-coding:utf-8-*-

import mysql.connector

SQL_GET_ALL_RECORD = "select count(1) from path_request_count;"
SQL_GET_SUM = "select sum(a.request_count) from (select request_count from path_request_count where path_id not in(0, -1) order by request_count desc limit {0}) a;"
SQL_INSERT_SUM = "insert into path_request_summary (path_count, request_sum) values (%s, %s);"

DB = "test_db"
HOST = "0.0.0.0"
PORT = "33333"
USER = "root"
PASSWORD = "root"


def main():
    print("START")
    summary()
    print("END")


def summary():

    all_records = get_all_record(SQL_GET_ALL_RECORD)

    for record in range(1, all_records[0]):
        sum = get_request_sum(SQL_GET_SUM.format(record))
        print(sum[0])
        insert_request_sum(record, sum[0])


def get_all_record(sql):
    return execute_select(sql)


def get_request_sum(sql):
    return execute_select(sql)


def insert_request_sum(record, sum):
    try:
        connection = getConnection()
        cursor = connection.cursor()
        cursor.execute(SQL_INSERT_SUM, (record, sum))
    except:
        print ("[SQL:!!!FAILED!!!]" + SQL_INSERT_SUM)
        sys.exit(1)
    finally:
        connection.commit()
        cursor.close()
        connection.close()


def execute_select(SQL_SELECT):
    connection = getConnection()
    try:
        cursor = connection.cursor()
        cursor.execute(SQL_SELECT)
        result = cursor.fetchone()
    except:
        result = None
        sys.exit(1)

    finally:
        cursor.close()
        connection.close()
    return result


def getConnection():
    try:
        connection = mysql.connector.connect(
            db=DB, host=HOST, port=PORT, user=USER, passwd=PASSWORD, buffered=True)
    except:
        print("HOST:" + HOST + " connetction error.")
        sys.exit(1)
    return connection


if __name__ == '__main__':
    main()
