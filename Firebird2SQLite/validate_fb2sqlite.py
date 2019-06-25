import fdb
import sqlite3
import sys
import os
from copy import copy


def list_comparison(list1, list2):
    t1 = copy(list1)
    t2 = copy(list2)

    retval_list = list()

    for item in list1:
        if item in list2:
            t1.remove(item)
            t2.remove(item)

    if not len(t1) == 0 and not len(t2) == 0:
        retval_list.append(t1)
        retval_list.append(t2)

    return retval_list if len(retval_list) > 0 else None


def query_database(fdb_cursor, sql_cursor, table, primary_key):
    global report_file

    assert isinstance(fdb_cursor, fdb.Cursor)
    assert isinstance(sql_cursor, sqlite3.Cursor)

    fdb_cursor.execute('select * from {0} {1}'.format(table, primary_key))
    fdb_columns = [description[0] for description in fdb_cursor.description]
    fdb_data = fdb_cursor.fetchall()

    sql_cursor.execute('select {0} from {1} {2}'.format(', '.join(fdb_columns), table, primary_key))
    sql_data = sql_cursor.fetchall()

    results = dict()
    validate_records(fdb_data, sql_data, fdb_columns, results)

    return results if results else None


def validate_records(fdb_records, sql_records, names, results):
    assert isinstance(fdb_records, (list, tuple))
    assert isinstance(sql_records, (list, tuple))
    assert isinstance(names, list)
    assert isinstance(results, dict)

    for index, item in enumerate(fdb_records):
        if isinstance(fdb_records, list):
            try:
                validate_records(item, sql_records[index], names, results)
            except IndexError:
                report_file.write("MISSING RECORDS")

        elif isinstance(fdb_records, tuple):
            if item and sql_records[index] and str(item) != str(sql_records[index]):

                try:
                    results[names[index]].append((str(item), str(sql_records[index])))
                except KeyError:
                    results[names[index]] = list()
                    results[names[index]].append((str(item), str(sql_records[index])))

    return results


def process_files(i_fb, i_sql):
    global report_file

    error_report = list()

    fb = fdb.connect(database=i_fb,
                     user='sysdba',
                     password='masterkey')

    fb_schema = fdb.schema.Schema()
    fb_schema.bind(fb)
    fb_cur = fb.cursor()

    sql = sqlite3.connect(i_sql)
    sql_cur = sql.cursor()

    report_file.write('--------------------------------\n')
    report_file.write('{0}\n'.format(os.path.splitext(os.path.basename(i_fb))[0]))
    report_file.write('================================\n\n')

    #Compare table names
    fb_tables = [item.name for item in fb_schema.tables]

    sql_cur.execute('select name from sqlite_master where type="table"')
    sql_tables = [item[0] for item in sql_cur.fetchall()]

    results = list_comparison(fb_tables, sql_tables)
    if isinstance(results, list):
        report_file.write("Identified table differences between Firebird and sqlite databases!\n")
        for item in results:
            report_file.write('{0}\n'.format(item))

    #Compare records in tables
    for item in fb_schema.tables:
        try:
            fdb_primary_keys = item.primary_key.index.segment_names
        except AttributeError:
            fdb_primary_keys = list()

        if fdb_primary_keys:
            pkeys = 'order by {0} desc'.format(', '.join(fdb_primary_keys))
        else:
            pkeys = ''

        #Compare primary keys
        sql_cur.execute('PRAGMA table_info({0})'.format(item.name))
        sql_table_info = sql_cur.fetchall()

        temp_keys = dict()
        for info in sql_table_info:
            if info[5] > 0:
                temp_keys[info[5]] = info[1]

        sql_primary_keys = [temp_keys[i] for i in sorted(temp_keys)]

        results = list_comparison(fdb_primary_keys, sql_primary_keys)

        if results:
            report_file.write('{0}\t\t\tPrimary Keys MISMATCHED!\n'.format(item.name))

        results = query_database(fb_cur, sql_cur, item.name, pkeys)

        if results:
            error_report.append('\n')

            error_report.append("Identified record differences between Firebird and sqlite databases on table {0}!\n"
                                .format(item.name))
            #report_file.write("Identified record differences between Firebird and sqlite databases on table {0}!\n"
            #                  .format(item.name))
            for key, value in results.iteritems():
                #report_file.write('Key: {0} (Firebird, sqlite)\n'.format(key))
                error_report.append('Key: {0} (Firebird, sqlite)\n'.format(key))
                for instance in value:
                    #report_file.write('{0}\n'.format(instance))
                    error_report.append('{0}\n'.format(instance))
            #report_file.write('\n')
            error_report.append('\n')

    report_file.write(''.join(error_report))

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Invalid number of parameters" \
              "Usage:   validate_fb2sqlite.py <firebird database> <sqlite database>" \
              "Example: validate_fb2sqlite.py database.gdb converteddb.db"
        sys.exit(1)

    if os.path.exists(sys.argv[2]):
        # Open report file
        report_file = open('report.txt', 'a+')
        process_files(sys.argv[1], sys.argv[2])
        report_file.close()

    else:
        sys.exit(2)
