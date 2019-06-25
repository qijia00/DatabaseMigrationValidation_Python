import fdb
import sqlite3
import copy

firebird = fdb.connect(database='C:/Users/a.mabini/Desktop/3.GDB',
                       user='sysdba',
                       password='masterkey')

sql = sqlite3.connect('C:/Users/a.mabini/Desktop/3.sql')
cur2 = sql.cursor()

print firebird

assert isinstance(firebird, fdb.Connection)

print firebird.database_info(fdb.isc_info_db_class, 's')

schema = fdb.schema.Schema()

schema.bind(firebird)

cur = firebird.cursor()

for table in schema.tables:
    assert isinstance(table, fdb.schema.Table)
    #print table.name, [c.name for c in table.columns]
    script = "select * from {0}".format(table.name)
    cur.execute(script)

    cur2.execute(script)

    print table.name

    data = cur.fetchall()
    tdata = copy.copy(data)

    data2 = cur2.fetchall()
    tdata2 = copy.copy(data2)

    if data:
        for item in data:
            if item in data2:
                tdata.remove(item)
                tdata2.remove(item)

        if len(tdata) > 0 or len(tdata2) > 0:
            print tdata
            print tdata2


def list_comparison(list1, list2):
    t1 = copy.copy(list1)
    t2 = copy.copy(list2)

    for item in list1:
        if item in list2:
            t1.remove(item)
            t2.remove(item)

    if not len(t1) == 0 and not len(t2) == 0:
        return t1, t2

