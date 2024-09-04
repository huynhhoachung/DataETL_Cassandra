# Cassandra ETL Pipeline for Music App

## Overview

This repository contains a Jupyter notebook for creating and populating Cassandra database tables using event data from a music application. The tables are designed to answer specific business questions.

### Part I: ETL Pipeline for Pre-Processing Files

Create List of Filepaths
```python
print(os.getcwd())
filepath = os.getcwd() + '/event_data'
file_path_list = glob.glob(os.path.join(filepath, '*'))
```
Process Files and Create CSV
```python
full_data_rows_list = []
for f in file_path_list:
    with open(f, 'r', encoding='utf8', newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)
        for line in csvreader:
            full_data_rows_list.append(line)

csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding='utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', 'level', 'location', 'sessionId', 'song', 'userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
```
Check CSV File
```python
with open('event_datafile_new.csv', 'r', encoding='utf8') as f:
    print(sum(1 for line in f))
```
### Part II: Loading Data into Cassandra Tables
Create Cassandra Cluster and Session
```python
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect()
```
Create Keyspace
```python
try:
    session.execute('''
    CREATE KEYSPACE IF NOT EXISTS music
    WITH REPLICATION =
    {'class': 'SimpleStrategy', 'replication_factor': 1}
    ''')
except Exception as e:
    print(e)
```
Set Keyspace
```python
session.set_keyspace('music')
```
Query 1: Artist, Song, and Length by Session and Item
```python
query = "CREATE TABLE IF NOT EXISTS session_item"
query += "(sessionId bigint, itemInSession bigint, artist text, song text, length float, PRIMARY KEY ((sessionId, itemInSession), artist, song, length))"
try:
    session.execute(query)
except Exception as e:
    print(e)

file = 'event_datafile_new.csv'
with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO session_item (sessionId, itemInSession, artist, song, length)"
        query += " VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))

query = '''SELECT artist, song, length FROM session_item WHERE sessionId=338 AND itemInSession=4'''
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
for row in rows:
    print(row.artist, row.song, row.length)
```
Query 2: Artist, Song, and User by User and Session
```python
query = "CREATE TABLE IF NOT EXISTS user_session"
query += "(userId bigint, sessionId bigint, itemInSession bigint, artist text, song text, user text, PRIMARY KEY ((userId, sessionId), itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(e)

file = 'event_datafile_new.csv'
with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO user_session (userId, sessionId, itemInSession, artist, song, user)"
        query += " VALUES (%s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1] + " " + line[4]))

query = "SELECT artist, song, user FROM user_session WHERE userId=10 AND sessionId=182"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
for row in rows:
    print(row.artist, row.song, row.user)
```
Query 3: Users Who Listened to a Particular Song
```python
query = "CREATE TABLE IF NOT EXISTS user_song"
query += "(song text, user text, PRIMARY KEY (song))"
try:
    session.execute(query)
except Exception as e:
    print(e)

file = 'event_datafile_new.csv'
with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO user_song (song, user)"
        query += " VALUES (%s, %s)"
        session.execute(query, (line[9], line[1] + ' ' + line[4]))

query = "SELECT user FROM user_song WHERE song='All Hands Against His Own'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
for row in rows:
    print(row.user)
```
Cleanup
```python
drop_query_0 = "drop table session_item"
drop_query_1 = "drop table user_session"
drop_query_2 = "drop table user_song"
try:
    session.execute(drop_query_0)
    session.execute(drop_query_1)
    session.execute(drop_query_2)
except Exception as e:
    print(e)
```
Close the Session and Cluster
```python
session.shutdown()
cluster.shutdown()
```

