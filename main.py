import re
from urllib.parse import urlparse, parse_qs
import mysql.connector 
from Classes import LogEntry, LogParser, DatabaseConnector


filename = 'log.txt'

# sql formula that has placeholders for 10 data extracted from the log, you can see what are these datas in the code:
sqlFormula = "INSERT INTO logs (ip_address, date_time, url, request_method, product_id, user_id,\
                                session_id, category, response_code, response_size) \
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

# database connection parameters :
db_host = "localhost"
db_user = "root"
db_password = "root"
db_name = "testdb"
bench_size = 10

# Initiating a DatabaseConnector object
db_connector = DatabaseConnector(db_host, db_user, db_password, db_name, bench_size)

#Initiating a LogParser object
log_parser = LogParser(filename)

# For loop which in each iteration will call the generator method of the log_parser object 
# and extract the data and pass it to the db_connector object as a tuple via LogEntry object methods
for entry in log_parser.parse_log_entries():
    if (entry != None):
        db_connector.write(sqlFormula, entry.get_all_tpl())
# making sure that there are no data left in the queue
db_connector.free_query_queue()

