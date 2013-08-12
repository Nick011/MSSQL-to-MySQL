#!/usr/bin/python
import MySQLdb
import pyodbc
import sys
import datetime

import includes.config as config
import includes.functions as functions
import sqlserver_datatypes.data_types as data_types

#connection for MSSQL. (Note: you must have FreeTDS installed and configured!)
ms_conn = pyodbc.connect(config.odbcConString)
ms_cursor = ms_conn.cursor()

#connection for MySQL
my_conn = MySQLdb.connect(host=config.MYSQL_host,user=config.MYSQL_user, passwd=config.MYSQL_passwd, db=config.MYSQL_db)
my_cursor = my_conn.cursor()

if listofTables:
    ms_tables = "','".join(map(str, listofTables))
    ms_tables = "WHERE name in ('"+ms_tables+"')"
else:
    ms_tables = ""

ms_cursor.execute("SELECT * FROM sysobjects %s" % ms_tables ) #sysobjects is a table in MSSQL db's containing meta data about the database. (Note: this may vary depending on your MSSQL version!)
ms_tables = ms_cursor.fetchall()
noLength = [56, 58, 61, 35] #list of MSSQL data types that don't require a defined lenght ie. datetime

for tbl in ms_tables:
    crtTable = tbl[0]
    ms_cursor.execute("SELECT * FROM syscolumns WHERE id = OBJECT_ID('%s')" % tbl[0]) #syscolumns: see sysobjects above.
    columns = ms_cursor.fetchall()
    attr = ""
    for col in columns:
        colType = data_types[str(col.xtype)] #retrieve the column type based on the data type id

        #make adjustments to account for data types present in MSSQL but not supported in MySQL (NEEDS WORK!)
        if col.xtype == 60:
            colType = "float"
            attr += "`"+col.name +"` "+ colType + "(" + str(col.length) + "),"
        elif col.xtype == 106:
            colType = "decimal"
            attr += "`"+col.name +"` "+ colType + "(" + str(col.xprec) + "," + str(col.xscale) + "),"            
        elif col.xtype == 108:
            colType = "decimal"
            attr += "`"+col.name +"` "+ colType + "(" + str(col.xprec) + "," + str(col.xscale) + "),"       
        elif col.xtype in noLength:
            attr += "`"+col.name +"` "+ colType + ","
        else:
            attr += "`"+col.name +"` "+ colType + "(" + str(col.length) + "),"
    
    attr = attr[:-1]

       
    if functions.check_table_exists(my_cursor, crtTable):
        my_cursor.execute("drop table "+crtTable)

    my_cursor.execute("CREATE TABLE " + crtTable + " (" + attr + ");") #create the new table and all columns
    ms_cursor.execute("SELECT * FROM "+ tbl[0])
    tbl_data = ms_cursor.fetchall()

    field_count = ", ".join("?" * len(columns))

    for row in tbl_data:
        new_row = list(row)

        for i in functions.common_iterable(new_row): 
       
            if new_row[i] == None:
    	       new_row[i] = 0
            elif type(new_row[i]) == datetime.datetime:
        
                new_row[i] = new_row[i].date().isoformat()
    			
        row = tuple(new_row)
        my_conn.ping(True)
       
        query_string = "INSERT INTO `" + crtTable + "` VALUES %r;" % (tuple(new_row),)
        
       
        my_cursor.execute(query_string)
        my_conn.commit() #mysql commit changes to database
      
my_cursor.close()
my_conn.close () #mysql close connection
conn.close() #mssql close connection
