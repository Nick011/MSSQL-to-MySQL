#!/usr/bin/python

#found this at http://stackoverflow.com/questions/12325608/iterate-over-a-dict-or-list-in-python

def common_iterable(obj):
    if isinstance(obj, dict):
        return obj
    else:
        return (index for index, value in enumerate(obj))

#found this at http://stackoverflow.com/questions/17044259/python-how-to-check-if-table-exists
def check_table_exists(dbcur, tablename):
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False
