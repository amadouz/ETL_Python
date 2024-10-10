# Comments on how to extract data from XML file 
# To extract from an XML file, you need first to parse the data from the file using the ElementTree function. You can then extract relevant information from this data and append it to a pandas dataframe as follows.
# Note: You must know the headers of the extracted data to write this function. In this data, you extract "name", "height", and "weight" headers for different persons.


import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines = True)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name": name, "height": height, "weight": weight}])], ignore_index = True)
    return dataframe

def extract():
    # Create empty dataFrame to hold extracted data
    extracted_data = pd.DataFrame(columns = ["name", "height", "weight"])

    # Process all CSV files
    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index = True)
    
    # Process all Json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index = True)

    # Process all xml files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index = True)

    
    return extracted_data

def transform(data):

    ''' Convert inches to meters and round off to two decimals 1 inch is 0.0254 meters'''
    data['height'] = round(data.height * 0.0254,2)

    ''' Convert pounds to kilograms and round off to two decimals 1 pound is 0.45359237 Kilograms'''
    data['weight'] = round(data.weight * 0.45359237, 2)

    return data


def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_process(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')

# Testing ETL operations and log progress

# Log the initialisation of the ETL process
log_process("ETL Job Started")

# Log the beginning of the Extraction process
extracted_data = extract()

# Log the completion of the Extraction process
log_process("Extract phase Ended")

# Log the beginning of the Transformation process
log_process("Transformation phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
log_process("Transformation phase Ended")

# Log the beginning of the Loading process
log_process("Load phase started")
load_data(target_file,transformed_data)

# Log the completion of the Loading process
log_process("Load phase Ended")

# Log the completion of the ETL process
log_process("ETL Job Ended")






wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip







2024-Jun-02-17:32:43,ETL Job Started
2024-Jun-02-17:32:43,Extract phase Ended
2024-Jun-02-17:33:18,ETL Job Started
2024-Jun-02-17:33:18,Extract phase Ended
2024-Jun-02-17:33:56,ETL Job Started
2024-Jun-02-17:33:56,Extract phase Ended
2024-Jun-02-17:33:56,Transformation phase Started
2024-Jun-02-17:34:44,ETL Job Started
2024-Jun-02-17:34:44,Extract phase Ended
2024-Jun-02-17:34:44,Transformation phase Started
2024-Jun-02-17:34:44,Transformation phase Ended
2024-Jun-02-17:34:44,Load phase started
2024-Jun-02-17:34:44,Load phase Ended
2024-Jun-02-17:34:44,ETL Job Ended
2024-Jun-02-17:36:36,ETL Job Started
2024-Jun-02-17:37:03,ETL Job Started
2024-Jun-02-17:37:27,ETL Job Started
2024-Jun-02-17:37:51,ETL Job Started
2024-Jun-02-17:38:31,ETL Job Started
2024-Jun-02-17:38:31,Extract phase Ended
2024-Jun-02-17:38:31,Transformation phase Started
2024-Jun-02-17:38:31,Transformation phase Ended
2024-Jun-02-17:38:31,Load phase started
2024-Jun-02-17:38:31,Load phase Ended
2024-Jun-02-17:38:31,ETL Job Ended






import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
URL = "https://hoopshype.com/salaries/players/"
r = requests.get(URL)
df = pd.read_html(r.content)
soup = BeautifulSoup(r.content, 'html5lib')
#print(soup.prettify())
tag_object = soup.div
print(tag_object.attrs)



#====================================================================================================================#



# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd 
import numpy as np 
from bs4 import BeautifulSoup
import requests
import sqlite3
from datetime import datetime

URL = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country','GDP_USD_millions']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'



def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''




#=================== Linux and Shell Scripting =========#

# Create a file and write the butter to the file 
sav example.txt 

# Write the butter to the file without exiting
w 

sudo apt update # Find packages to upgrade in the system

sudo yum update # Update all packages out of date in the system

sudo apt install <package> # To install a new package

ps -e # Processes running in the system

top -n 3 # Top 3 running processes

date
date "+%j day of %Y" # 097 day of 2023; %A day of week
man <command> # manual of the command <command>
pwd
ls 
cd 
find . -iname "filename.extensioin"
pwd
ls
rm file1
ls
rm forlder1
rm -r folder1 
ls
cp filename directory
cp -r directorySource directoryDestination
mv fileSource directoryDestination 
mv directorySource directoryDestination
cat # Print entire file contents
more filename # print file contents page-by-page
head filename or head -n 5
tail filename
cat filename #
wc filename # nb lines nb word nb characters
wc -l filename # only line count
wc -c # carac. count
sort filename # Alphabetical sort the content
sort -r filename # reverse order 
uniq filename # remove duplicated lines
grep ch filename # return matching lines
cat -c -9 filename # remove caracthers
cat -d ' ' -f2 filename # Cutting to extract second field (last_name)
paste filename1 filename2 filename3 # Merge lines from multiple files
paste -d "," filename1 filename2 # Merge using delimitor ,
ifconfig # Display network interfaces or configure
infconfig eth0 # Information about the information adapter
ping link # Send ICMP packets to URL and print response
wget or curl link # Download a file ; s
ls -r 
tar -cf notes.tar notes # Archive and extrat files
tar -czf notes.tar.gz notes # To be too compressed   z
tar -xf notes.tar notes # Extract in a folder noes
ls -R # To see the extracted files and directories
tar -xzf notes.tar.gz notes # Deecompress in notes 
zip # Compresss files and directories to an archive
zip -r notes.zip notes # Create zip file 
unzip filename # Decompress the file filename 
$(command) ex: here = $(pwd) # show the content of the command here: /home/jgrom
command1; command2 # Running sequential mode
command1 & command2 # Running in parralel
contrab -e # Open the editor to schedule a job
# Job Syntax: m h dom mon dow command ex: 30 15 * * 0 date >> sundays.txt

# Create a new Bash script 
echo '#!/bin/bash' > conditional_script.sh
chmod u+x conditional_script.sh

# Query the user and save response
#!/bin/bash
echo 'Are you enjoying this course so far?'
echo -n "Enter \"y\" for yes, \"n\" for no."
read response

# Math calculation bash
#!/bin/bash
echo -n "Enter an integer: "
read n1
echo -n "Enter another integer: "
read n2
sum=$(($n1+$n2))
product=$(($n1*$n2))
echo "The sum of $n1 and $n2 is $sum"
echo "The product of $n1 and $n2 is $product."
# Adding logic 
#!/bin/bash
echo -n "Enter an integer: "
read n1
echo -n "Enter another integer: "
read n2
sum=$(($n1+$n2))
product=$(($n1*$n2))
echo "The sum of $n1 and $n2 is $sum"
echo "The product of $n1 and $n2 is $product."
if [ $sum -lt $product ]
then
   echo "The sum is less than the product."
elif [[ $sum == $product ]]
then
   echo "The sum is equal to the product."
elif [ $sum -gt $product ]
then
   echo "The sum is greater than the product."
fi

# File source : https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-LX0117EN-SkillsNetwork/labs/M3/L2/arrays_table.csv
# Bash Script that Parses table columns into 3 arrays
#!/bin/bash
csv_file="./arrays_table.csv"
# parse table columns into 3 arrays
column_0=($(cut -d "," -f 1 $csv_file))
column_1=($(cut -d "," -f 2 $csv_file))
column_2=($(cut -d "," -f 3 $csv_file))
# print first array
echo "Displaying the first column:"
echo "${column_0[@]}"


# Create a new array as the difference between the third and Second columns
#!/bin/bash
csv_file="./arrays_table.csv"
# parse table columns into 3 arrays
column_0=($(cut -d "," -f 1 $csv_file))
column_1=($(cut -d "," -f 2 $csv_file))
column_2=($(cut -d "," -f 3 $csv_file))
# print first array
echo "Displaying the first column:"
echo "${column_0[@]}"
## Create a new array as the difference of columns 1 and 2
# initialize array with header
column_3=("column_3")
# get the number of lines in each column
nlines=$(cat $csv_file | wc -l)
echo "There are $nlines lines in the file"
# populate the array
for ((i=1; i<$nlines; i++)); do
  column_3[$i]=$((column_2[$i] - column_1[$i]))
done
echo "${column_3[@]}"


# Create a Report by combining your new column with the source table 

#!/bin/bash

csv_file="./arrays_table.csv"

# parse table columns into 3 arrays
column_0=($(cut -d "," -f 1 $csv_file))
column_1=($(cut -d "," -f 2 $csv_file))
column_2=($(cut -d "," -f 3 $csv_file))

# print first array
echo "Displaying the first column:"
echo "${column_0[@]}"

## Create a new array as the difference of columns 1 and 2
# initialize array with header
column_3=("column_3")
# get the number of lines in each column
nlines=$(cat $csv_file | wc -l)
echo "There are $nlines lines in the file"
# populate the array
for ((i=1; i<$nlines; i++)); do
  column_3[$i]=$((column_2[$i] - column_1[$i]))
done
echo "${column_3[@]}"

## Combine the new array with the csv file
# first write the new array to file
# initialize the file with a header
echo "${column_3[0]}" > column_3.txt
for ((i=1; i<nlines; i++)); do
  echo "${column_3[$i]}" >> column_3.txt
done
paste -d "," $csv_file column_3.txt > report.csv
































# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd 
import sqlite3 
import numpy as np 
from datetime import datetime

URL = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name','MC_USD_Billion']
table_attributes_final = ['Name','MC_USD_BILLION','MC_GBP_BILLION','MC_EUR_BILLION','MC_INR_BILLION']
db_name = 'banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
csv_file = 'exchange_rate.csv'
log_file = 'code_log.txt'
sql_connection = sqlite3.connect('Banks.db')

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp +' : ' + message+ '\n')

# Log the initialisation of the ETL process
log_progress("Preliminaries complete. Initiating ETL process")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    df = pd.DataFrame(columns=table_attribs)
    r = requests.get(URL)
    soup = BeautifulSoup(r.content, 'html5lib')
    rows = soup.find_all('table')[0].find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        if len(cells) != 0:
           data_dict = {
            table_attribs[0] : cells[1].text.strip(),
            table_attribs[1] : float(cells[2].text.strip())
           }
           df = pd.concat([df,pd.DataFrame(data_dict, index=[0])], ignore_index = True)

    return df

extracted_data = extract(URL,table_attribs)
# Log the completion of data extraction
log_progress("Data extraction complete. Initiating Transformation process")

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate = pd.DataFrame(data = pd.read_csv(csv_path),columns = ['Currency','Rate'])
    exchange_rate_dict = exchange_rate.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate_dict['EUR'],2) for x in df['MC_USD_Billion']]  
    df['MC_INR_Billion'] = [np.round(x*exchange_rate_dict['INR'],2) for x in df['MC_USD_Billion']]  

    return df

transformed_data = transform(extracted_data,csv_file)
# Log the end of Data transformation
log_progress("Data transformation complete. Initiating Loading process")

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)


load_to_csv(transformed_data,csv_path)
# Log the data saving in the CSV file 
log_progress("Data saved to CSV file")

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

load_to_db(transformed_data,sql_connection,table_name)
# Log the DB connection
log_progress("SQL Connection initiated")

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    return pd.read_sql(query_statement,sql_connection)


#run_query(query_statement, sql_connection)
print(run_query("SELECT * FROM Largest_banks", sql_connection))

print(run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", sql_connection))

print(run_query("SELECT Name from Largest_banks LIMIT 5", sql_connection))

# Log the completion of SQL execution 
log_progress("Process Complete")


# Log Closing of DB Connection
log_progress("Server Connection closed")







Hands-on Lab: Acquiring and Processing Information on the World's Largest Banks

Estimated Time: 60 mins
In this project, you will put all the skills acquired throughout the course and your knowledge of basic Python to test. You will work on real-world data and perform the operations of Extraction, Transformation, and Loading (ETL) as required.

Disclaimer:

Cloud IDE is not a persistent platform, and you will lose your progress every time you restart this lab. We recommend saving a copy of your file on your local machine as a protective measure against data loss.

Project Scenario:
You have been hired as a data engineer by research organization. Your boss has asked you to create a code that can be used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, the data needs to be transformed and stored in GBP, EUR and INR as well, in accordance with the exchange rate information that has been made available to you as a CSV file. The processed information table is to be saved locally in a CSV format and as a database table.

Your job is to create an automated system to generate this information so that the same can be executed in every financial quarter to prepare the report.

Particulars of the code to be made have been shared below.

Parameter	Value
Code name	banks_project.py
Data URL	https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks
Exchange rate CSV path	https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv
Table Attributes (upon Extraction only)	Name, MC_USD_Billion
Table Attributes (final)	Name, MC_USD_Billion, MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion
Output CSV Path	./Largest_banks_data.csv
Database name	Banks.db
Table name	Largest_banks
Log file	code_log.txt
Project tasks
Task 1:
Write a function log_progress() to log the progress of the code at different stages in a file code_log.txt. Use the list of log points provided to create log entries as every stage of the code.

Task 2:
Extract the tabular information from the given URL under the heading 'By market capitalization' and save it to a dataframe.
a. Inspect the webpage and identify the position and pattern of the tabular information in the HTML code
b. Write the code for a function extract() to perform the required data extraction.
c. Execute a function call to extract() to verify the output.

Task 3:
Transform the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
a. Write the code for a function transform() to perform the said task.
b. Execute a function call to transform() and verify the output.

Task 4:
Load the transformed dataframe to an output CSV file. Write a function load_to_csv(), execute a function call and verify the output.

Task 5:
Load the transformed dataframe to an SQL database server as a table. Write a function load_to_db(), execute a function call and verify the output.

Task 6:
Run queries on the database table. Write a function load_to_db(), execute a given set of queries and verify the output.

Task 7:
Verify that the log entries have been completed at all stages by checking the contents of the file code_log.txt.




#====================== EXAM 

# Import libraries required for connecting to mysql
import mysql.connector

# Import libraries required for connecting to DB2 or PostgreSql
import psycopg2

# Connect to MySQL
connection = mysql.connector.connect(user='root', password='lSOIHEEODkdCGx7PpdijPpWk',host='172.21.80.142',database='sales')

# Connect to DB2 or PostgreSql
dsn_hostname = '127.0.0.1'
dsn_user='postgres'        # e.g. "abc12345"
dsn_pwd ='Mjc2NDQtdGF6ZHlt'      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port ="5432"                # e.g. "50000" 
dsn_database ="postgres"           # i.e. "BLUDB"


# create connection

conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port
)

# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

def get_last_rowid():
    cursor = conn.cursor()
    cursor.execute('SELECT rowid FROM sales_data order by rowid desc limit 1;')
    rowid = cursor.fetchall()
    conn.commit()
    return rowid[0][0]


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    SQL = "SELECT * FROM sales_data where rowid > '%rowid'" %rowid
    cursor = connection.cursor()
    cursor.execute(SQL)
    records = cursor.fetchall()
    return records  

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records):
    cursor = conn.cursor()
    for record in records:
       SQL = "INSERT INTO sales_data(rowid,productid,customerid,price,quantity,timeestamp) values(%s,%s,%s,%s,%s,%s)"
       cursor.execute(SQL,record);
       conn.commit()
       
insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
connection.close()
# disconnect from DB2 or PostgreSql data warehouse 
conn.close()

# End of program






