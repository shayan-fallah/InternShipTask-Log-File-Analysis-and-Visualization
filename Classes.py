import re
from urllib.parse import urlparse, parse_qs
import mysql.connector 


# LogEntry object is going to recieve the extracted and parsed data from the LogParser object and returns each parameter by
# the defined methods
class LogEntry:
    def __init__(self, ip_address, date_time,  url_path,  request_method, query_params, response_code, response_size):
        self.ip_address = ip_address
        self.date_time = date_time
        self.url_path = url_path
        self.request_method = request_method
        self.query_params = query_params
        self.response_code = response_code
        self.response_size = response_size

    def get_ip_address(self):
        return self.ip_address[0]
    
    def get_date_time(self):
        return self.date_time[0]
    
    def get_url_path(self):
        return self.url_path
    
    def get_request_method(self):
        return self.request_method
    
    def get_query_params(self):
        indv_query_par = {'user_id': '-', 'session_id' : '-', 'product_id' : '-', 'category' : '-'}
        for key in self.query_params.keys():
            indv_query_par[key] = ', '.join(self.query_params[key])
        return indv_query_par
    
    def get_response_code(self):
        return self.response_code
    
    def get_response_size(self):
        return self.response_size
    
    def get_all_tpl(self):
        tpl = (self.get_ip_address(),
            self.get_date_time(),
            self.get_url_path(),
            self.get_request_method(),
            self.get_query_params()['user_id'],
             self.get_query_params()['session_id'],
            self.get_query_params()['product_id'],
             self.get_query_params()['category'],
            self.get_response_code(),
            self.get_response_size())
        return tpl

#LogParser object will be initiated by passing the txt file location
#It has a parse_log_entries method which is a generator,yielding an instance of LogEntry object 
#this object contains the extracted data which is done by the _parse_log_line method of this class
# for each line in the txt file and finally returns this data in a from of LogEntry object 
class LogParser:
    def __init__(self, log_file_path):
        self.log_file_path = log_file_path

    def parse_log_entries(self):
        with open(self.log_file_path, 'r') as log_file:
            for line in log_file:
                yield self._parse_log_line(line)

    def _parse_log_line(self, log_line):
        if(log_line.split()[0] == "NULL"):   # if Null return None
            return
        
        ip_address_pattern = r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'  #IP Adresses
        ip_address = re.findall(ip_address_pattern, log_line)
        
        date_time_pattern = r'\[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\]' #Date Time
        date_time = re.findall(date_time_pattern, log_line)
    

        parsed_url = urlparse(log_line.split('"')[1]) #URL Paths
        url_path = parsed_url.path.split()[1]

        query_param = parse_qs(parsed_url.query)  #query Parameters
        for key in query_param.keys():
            for i in range(len(query_param[key])):
                query_param[key][i] = query_param[key][i].split()[0] 

        response_code = log_line.split()[-2]    #Response Codes
        
        response_size = log_line.split()[-1]    #Response Size

        request_method_pattern = r'"(GET|POST|PUT|DELETE)'  #Request Methods
        match = re.search(request_method_pattern, log_line)
        request_method = match.group(1)
        
        LogInstance = LogEntry( ip_address,
                                date_time,
                                url_path,
                                request_method,
                                query_param,
                                response_code,
                                response_size
                                )
        return LogInstance


# The object of this class will be initiated with the connection params for the mysql database
# connect method will start a connection with the mysql server
# write method will recieve a sqlformula and a tuple which contains the params for the holders in the sqlformula
#   it will wait for ten querys and then it will stablish a connection via connect method and send the data to the database
#     I decided to put this 10 query boundry since this database is not a real time database to reduce the number of connections
# finally the free_query_queue will be called at the end of the main code to make sure there is no data left in the queue 
class DatabaseConnector:
    def __init__(self, host, user, password, database, batch_size):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.batch_size = batch_size
        self.query_count = 0
        self.query_queue = []

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            print("Connected to the database successfully!")
        except mysql.connector.Error as err:
            print(f"Error connecting to the database: {err}")

    def write(self, sqlFormula, tpl):
        try:
            if(len(self.query_queue) < self.batch_size):
                self.query_queue.append([sqlFormula, tpl])
            else:
                self.query_queue.append([sqlFormula, tpl])
                self.connect()
                cursor = self.connection.cursor()
                for query in self.query_queue:
                    cursor.execute(query[0], query[1])
                    self.commit_changes()
                    print(f"{query[1]} is commited")
                    self.query_count += 1
                self.query_queue.clear()
                self.close_connection()
                return cursor.fetchall()
                
        except mysql.connector.Error as err:
            print(f"Error executing query: {err}")
            return None
    def free_query_queue(self):
        try:
            self.connect()
            cursor = self.connection.cursor()
            for query in self.query_queue:
                cursor.execute(query[0], query[1])
                self.commit_changes()
                print(f"{query[1]} is commited")
                self.query_count += 1
            self.query_queue.clear()
            self.close_connection()
            return cursor.fetchall()
        except mysql.connector.Error as err:
            print(f"Error executing query: {err}")
            return None
    def commit_changes(self):
        try:
            self.connection.commit()
            print("Changes committed successfully!")
        except mysql.connector.Error as err:
            print(f"Error committing changes: {err}")

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("Database connection closed.")
