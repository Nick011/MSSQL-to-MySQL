#!/usr/bin/python
import MySQLdb
import pyodbc

typesFile = open('sqlserver_datatypes.txt', 'r').readlines()
dataTypes = dict((row.split(',')[0].strip(),row.split(',')[1].strip()) for row in typesFile)

#connection for MSSQL. (Note: you must have FreeTDS installed and configured!)
conn = pyodbc.connect('DRIVER={FreeTDS}; SERVER=YOUR SERVER; DATABASE=YOUR DB; UID=YOUR ID; PWD=YOUR PASS')
msCursor = conn.cursor()

#connection for MySQL
db = MySQLdb.connect(passwd="YOUR PASS", db="YOUR DB")
myCursor = db.cursor()

msCursor.execute("SELECT * FROM sysobjects WHERE type='U'") #sysobjects is a table in MSSQL db's containing meta data about the database. (Note: this may vary depending on your MSSQL version!)
dbTables = msCursor.fetchall()
noLength = [56, 58, 61] #list of MSSQL data types that don't require a defined lenght ie. datetime
for tbl in dbTables:
    print 'migrating {0}'.format(tbl[0])
    msCursor.execute("SELECT * FROM syscolumns WHERE id = OBJECT_ID('%s')" % tbl[0]) #syscolumns: see sysobjects above.
    columns = msCursor.fetchall()
    attr = ""
    for col in columns:
	colType = dataTypes[str(col.xtype)] #retrieve the column type based on the data type id

	#make adjustments to account for data types present in MSSQL but not supported in MySQL (NEEDS WORK!)
	if col.xtype == 60:
	    colType = "float"
	    attr += col.name +" "+ colType + "(" + str(col.length) + "),"
	elif col.xtype in noLength:
	    attr += col.name +" "+ colType + ","
	else:
	    attr += col.name +" "+ colType + "(" + str(col.length) + "),"
	
    attr = attr[:-1]
   
    print 'Fetch rows from table {0}'.format(tbl[0])
    myCursor.execute("CREATE TABLE " + tbl[0] + " (" + attr + ");") #create the new table and all columns
    msCursor.execute("select * from %s" % tbl[0])
    tblData = msCursor.fetchmany(1000)

    while len(tblData) > 0:
        cnt = 0
        #populate the new MySQL table with the data from MSSQL
        for row in tblData:
    	    fieldList = ""
    	    for field in row:
	        if field == None: 
		    fieldList += "NULL,"
	        else:
		    field = MySQLdb.escape_string(str(field))
		    fieldList += "'"+ field + "',"

	    fieldList = fieldList[:-1]
	    myCursor.execute("INSERT INTO " + tbl[0] + " VALUES (" + fieldList + ")" )
            cnt += 1
            if cnt%100 == 0:
                print 'inserted 100 rows into table {0}'.format(tbl[0])
        db.commit()
        tblData = msCursor.fetchmany(1000)


