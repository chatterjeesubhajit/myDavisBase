# myDavisBase

## A light implementation of MySQL database engine  

## Inspiration
This project was developed as part of a coursework requirement. It covers a rudimentary implementation of MySQL database engine using *file-per-table* approach for physical storage, where each database table is stored as a separate file

## What it does

 - This data base engine allows user to perform basic DDL (Data Definition Language) commands like creating table, show tables and drop table
 - It allows DML (Data Manipulation Language) commands like Insert (records) into table ; Delete from table and Update records in a table
 - It allows QDL (Query Definition Language) commands like Select with up to two where conditions 
 - Below screenshot shows the list of commands that are supported:

![alt text](https://github.com/chatterjeesubhajit/myDavisBase/blob/main/Screenshot1.PNG)
 
## How I built it

- This application is built in Java with IntelliJ IDE.
- *file-per-table* approach is adopted , where each table is stored as single file.
- Each table is further divided into *Pages* (logical sections) of a fixed equal size (default as 512B)
- The Pages are stored in a B+ Tree structure to allow efficient storage and retrieval of data 
- The interface is command line based , with a `sql>` prompt
- The database metadata is maintained using two catalog files corresponding to two tables:
    -*davidbase_tables* : containing details of all tables stored in the database
    -*davidbase_columns* : containing details of all columns associated with the tables in the database
- The two catalog files are used to maintain the metadata of tables and also as a entry point for the root page for each table , i.e. root of the B+ tree per table file
- `SELECT` queries with up to two `WHERE` conditions are implemented 
- Constraints like Primary Key, Is Nullable, Unique, are also allowed for table columns
- The details of the `Page` and `cell` format can be found in the uploaded `DavisBase Nano File Format Guide (SDL)(1).pdf` file



## Challenges
- The major challenge was to implement the logical pages for each file and build the B+ tree structure, which holds data nodes only at the leaves of the tree
- Syncing up the catalog table with the root page number , each time data is added / removed was also tedious to design
- The most interesting part was implement the `WHERE` condition with up to two conditions , wherein I implemented separate sub-query for each condition and then a union/intersection/subtraction output from the two datasets is finally written to the data
- Also, choosing the correct records deletion mechanism was challenging. Since, same page locations weren't not to be allowed for re-written after a record is deleted, these positions were to be marked accordingly in pages, so they are avoided from further reads/writes
- Implementing the storage format of the records (mentioned in the file uploaded) for different types of data of (TEXT, INTEGER, FLOAT, DOUBLE, LONG , etc.) and implementing the constraints during record insertions were  challenging to code as well









    








