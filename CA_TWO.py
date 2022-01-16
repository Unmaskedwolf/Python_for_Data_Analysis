import pypyodbc
import numpy as np
import pandas as pd

# Load the CSV file (using Python)
df = pd.read_csv('FireBrigadeAndAmbulanceCallOuts.csv')
# checking whether the data is loaded correctly
print("Display first 10 rows of the dataframe :")
print(df.head(10))

# Output the total number of rows and columns.
print("Rows : {} columns : {}".format(df.shape[0], df.shape[1]))

# Output the number of non-null rows (by column).
print("Total count of non null rows by each column : \n", df.count())

# Output the number of null values (by column).
print("Total count of null values by each column : \n", df.isnull().sum())

# Output the number of null values for all columns.
print("Total count of null values of all the columns : ", df.isnull().sum().sum())

# Output the total number of call outs by Station Area.
print("Count of call outs by Station Area : ")
print(df.groupby('Station Area').size())

# Output the total number of call outs by Date and Station Area.
print("Total number of call outs by Date and Station Area :")
print(df.groupby(['Date', 'Station Area']).size())

# Output the total number of call outs by Station Area and Date
# where the description is either Fire Car or Fire Alarm.
print(df.loc[(df['Description'] == 'Fire CAR')
      | (df['Description'] == 'Fire ALARM')].groupby(['Station Area', 'Date']).size())

# Replace any instance of “,” (in any column) with an empty string.
df.replace(',', '', regex=True, inplace=True)

# Replace any instance of “-” (in any column) with an empty string.
df.replace('-', '', regex=True, inplace=True)

# Drop rows for the columns (AH, MAV, CD) where at least one row value is NULL.
df.dropna(subset=['AH', 'MAV', 'CD'], inplace=True)

# Drop any duplicate rows (except for the first occurrence).
df.drop_duplicates(keep='first', inplace=True)

# replacing NaN and empty string to None to avoid issues in SQL
df.replace(np.NaN, '', regex=True, inplace=True)
df.replace('', None, regex=True, inplace=True)

print(df)

# Output the minimum time difference between TOC and ORD.
tocTimes = pd.to_datetime(df['TOC'], format='%H:%M:%S')
ordTimes = pd.to_datetime(df['ORD'], format='%H:%M:%S')
tocTimes = tocTimes.min()
ordTimes = ordTimes.min()
tocTimes = tocTimes.to_pydatetime()
ordTimes = ordTimes.to_pydatetime()
diff = ordTimes - tocTimes
print("The minimum time difference between TOC and ORD: ", diff)


# Using the resulting data set, post implementing the previous cleansing steps, load the data into a table in SQL Server.
# (Note: you must create the physical table in SQL Server to complete this task. Use the same column names
# as the columns in the CSV File.)


# check driver in pypyodbc.drivers()
for driver in pypyodbc.drivers():
    print(driver)

# Server=localhost\SQLEXPRESS;Database=master;Trusted_Connection=True;
server = 'localhost\SQLEXPRESS'
database = '10595913'
connection = pypyodbc.connect(
    "Driver={SQL Server}; Server=" + server + "; Database=" + database + "; Trusted_Connection=yes;")


cursor = connection.cursor()
# handling table creation
cursor.execute(
    "IF OBJECT_ID('FireBrigadeAndAmbulanceCallOuts', 'U') IS NOT NULL DROP TABLE FireBrigadeAndAmbulanceCallOuts")


sql_command = 'CREATE TABLE FireBrigadeAndAmbulanceCallOuts (Date VARCHAR(50), StationArea VARCHAR(50), Description VARCHAR(50), TOC TIME, ORD TIME, MOB TIME, IA TIME, LS TIME, AH TIME, MAV TIME, CD TIME)'
cursor.execute(sql_command)
connection.commit()

# converting all vallues in the dataframe to list
values = df.values.tolist()
for row in values:
    sql_command = 'INSERT INTO FireBrigadeAndAmbulanceCallOuts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    cursor.execute(sql_command, row)
    connection.commit()

# read the data from table in SQL Server and print the first 10 rows
sql_command = 'SELECT TOP 5 * FROM FireBrigadeAndAmbulanceCallOuts'
cursor.execute(sql_command)
# convert to dataframe
df1 = pd.DataFrame()

# insert list of values into dataframe
df1 = pd.DataFrame(cursor.fetchall(), columns=['Date', 'Station Area', 'Description',
                   'TOC', 'ORD', 'MOB', 'IA', 'LS', 'AH', 'MAV', 'CD'])
print(df1.head(10))

connection.close()
