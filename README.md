# InternShipTask-Log-File-Analysis-and-Visualization
This project aims to build a data pipeline that reads nginx log files, parses and cleans the data, stores it in a database, and generates visualizations to extract insights

## Classes.py 
  this file contains three classes including `LogEntry`, `LogParser` and `DatabaseConnector`.
  `LogEntry` object will return the extracted data sepratly and all at once 
  `LogParser` object is has a generator method which yield each line of the txt file
    and extract data and return it as a `LogEntry` object
  `DatabaseConnector` is for stablishing connection with mysql server and stroing the 
    extracted data

## main.py 
  contain the main code which extract the data and store it in the data base

## Visulization.ipynb
  jupyter nootbook contains graph and visulization of the extracted data 

## log.json
  json file which I used the mysql workbench to export the database that the python script created from log.txt
