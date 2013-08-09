MSSQL-to-MySQL is a quick and dirty python script for cloning a MSSQL database to MySQL.
The script executes well enough for my needs, but could be greatly improved by supporting all necessary data type conversions from MSSQL to MySQL, as MySQL doesn't support all the types provided by MSSQL.


Requirements:
- Python 2.6 or higher
- MySQLdb (easy_install mysql-python)
- pyodbc (http://code.google.com/p/pyodbc/)
- FreeTDS (http://www.freetds.org/)*

*FreeTDS may require additional configuration beyond installation. I had to modify /etc/odbc.ini and odbcinst.ini to point at my FreeTDS installation.

Instructions:
- Install above requirements.
- Modify the includes/conifg.py script to include the connection information for your source MSSQL database and your destination MySQL database, and make a list of the tables to re-create
- Upload script and files to your server and run it. If you don't receive an error, the conversion was successful, congratulations!
If you do receive an error, start de-bugging and fork your changes or submit a pull request.

