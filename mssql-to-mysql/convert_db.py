#!/usr/bin/python
import MySQLdb
import pyodbc
import sys
import datetime

import includes.config as config
import includes.functions as functions
import sqlserver_datatypes as data_types

dataTypes = data_types.data_types

#connection for MSSQL. (Note: you must have FreeTDS installed and configured!)
msConn = pyodbc.connect(config.odbcConString)
msCursor = conn.cursor()

#connection for MySQL
myConn = MySQLdb.connect(host=config.MYSQL_host,user=config.MYSQL_user, passwd=config.MYSQL_passwd, db=config.MYSQL_db)
myCursor = myConn.cursor()

if listofTables:
    strofTables = "','".join(map(str, listofTables))
    strofTables = "('"+strofTables+"')"
else:
    strofTables = "*"

msCursor.execute("SELECT * FROM sysobjects WHERE name in %s" % strofTables ) #sysobjects is a table in MSSQL db's containing meta data about the database. (Note: this may vary depending on your MSSQL version!)
dbTables = msCursor.fetchall()
noLength = [56, 58, 61, 35] #list of MSSQL data types that don't require a defined lenght ie. datetime

for tbl in dbTables:
    crtTable = tbl[0]
    msCursor.execute("SELECT * FROM syscolumns WHERE id = OBJECT_ID('%s')" % tbl[0]) #syscolumns: see sysobjects above.
    columns = msCursor.fetchall()
    attr = ""
    for col in columns:
        colType = dataTypes[str(col.xtype)] #retrieve the column type based on the data type id

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

       
    if functions.checkTableExists(myCursor, crtTable):
        myCursor.execute("drop table "+crtTable)

    myCursor.execute("CREATE TABLE " + crtTable + " (" + attr + ");") #create the new table and all columns
    msCursor.execute("select * from "+ tbl[0])
    tblData = msCursor.fetchall()

    fieldcount = ", ".join("?" * len(columns))

    for row in tblData:
        newrow = list(row)

        for i in functions.common_iterable(newrow): 
       
            if newrow[i] == None:
    	       newrow[i] = 0
            elif type(newrow[i]) == datetime.datetime:
        
                newrow[i] = newrow[i].date().isoformat()
    			
        row = tuple(newrow)
        myConn.ping(True)
       
        query_string = "insert into `" + crtTable + "` VALUES %r;" % (tuple(newrow),)
        
       
        myCursor.execute(query_string)
        myConn.commit() #mysql commit changes to database
      
myCursor.close()
myConn.close () #mysql close connection
conn.close() #mssql close connection
