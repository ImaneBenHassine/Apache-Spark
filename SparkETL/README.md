### 1. Spark & SQL
#### 1.1 Install SQL
Downloading [SQL Server 2022 Developer](https://www.microsoft.com/fr-fr/sql-server/sql-server-downloads) which is the **Developper** version for SQLServer.

From the SQL Server Installation Server we lunch a system configuration checker to make sure we have evry thing we need to run SQL Server.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/e410f651-609c-45d7-babd-84e942858275)

Next we go to Installation and lunch the download of SQL Server Management Tools, it will reset us automatically to : https://learn.microsoft.com/fr-fr/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-ver16

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/9f1c58c2-cda7-4cc2-bc06-cb3444fa4fab)

And from there, we go through the instructions and choose what we need for the project.

#### 1.2 Create User & DataBase 
Now we create a new databse, a user and we grant all the previleges on this databse to the new user already created.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/0083ddcb-0f1d-4966-8ec3-ee32b08a5857)

Then we create our first table and load the data from our csv file :

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/b2180489-7bcd-4dc0-85fc-c56c8d31f84c)

a quick check of the data in the table 

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/7bc2833b-14c8-47df-ad5a-e4f78980a7f0)

#### 1.3 Create Connection

In order to connect to SQL-server (mssql) from Apache Spark, we would need the following :

- Driver to use (I will provide this)
- SQL server address & port
- Database name
- Table name
- User name and
- Password
  
the Steps to connect Spark to SQL Server and Read and write Table are: 

Step 1 – Identify the Spark SQL Connector version to use : Apache Spark connector for SQL server works with both **SQL Server on-prem** & **Azure SQL**

Step 2 – Add the dependency : I am using Maven, so I add the  dependency to the pom.xml

Step 3 – Create SparkSession & Dataframe

Step 4 – Save Spark DataFrame to SQL Server Table

Step 5 – Read SQL Table to Spark Dataframe

Make sure you have these details before you read or write to the SQL server. The driver I am going to use in this article is **com.microsoft.sqlserver.jdbc.spark**


### 2. Spark & MySQL

[The PySpark code on Jupyter Notebook](https://github.com/ImaneBenHassine/Apache-Spark/blob/main/SparkETL/Spark%20%26%20MySQL.ipynb)

#### 2.1 Install MySQL

Download the Community (GPL) version of MySQL for Windows from the link : https://dev.mysql.com/downloads/file/?id=518835

Then create the "root" account while installing to test the first conncetion !
![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/e44d085e-e1c2-4289-b733-37d07b5e1fd7)

Now we need to create a "user" account so we can have access to the database.

#### 2.2 Create User & DataBase 
here we create a new databse:

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/d30b34f7-3bf2-4f23-8301-f22df2dee8b0)

then we add the user and we grant all the previleges on this databse to the new user already created

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/7c5cabbb-b6fc-4bcd-a23c-7026d4c68469)

Then we create our first table :
![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/7d0db253-3935-4efd-9f5f-8fe2b1f0f5c1)

Now we charge the data using :

      LOAD DATA 
         LOCAL INFILE 'C://..//Orders.csv'
         INTO TABLE orders
         fields terminated by ','
         lines terminated by '\r\n'
         ignore 1 lines;
     
but we have an Error Code: 3948. Loading local data is disabled; this must be enabled on both the client and server sides	0.000 sec. So we need to activate the option to charge the data locally so we use **MySQL Command Line**.

First we get into the directory where we installed MySQL under the bin of the Server file, we execute this ligne to  access the  MySQL monitor and connecting with the administrator account and it's pwd created from the installation :

        mysql -u root -p

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/62c38593-ca7b-4b9f-bc4d-4cf858814ed3)

to show the database and the empty table that we created earlier : 

    show database;
    select * from spark_db.orders;

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/4b7e4fe7-514e-4797-98d6-79aa3e945007)

To activate the option of loading the data in local  we need to set up the variable **local_infile** to **'ON'** instead of  **'OFF'**

     mysql> set global local_infile=1;

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/d06c1775-5dae-4dc3-aaeb-88d68a00d612)

this was side **Client**!
Now let's change it side **Server**. So we exist anr reconnect to MySql but considering the **local_infile** set to **'ON'** before connecting to the root and typing the passeword :

     mysql --local-infile=1 -u root -p

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/2d1b26af-d147-44e7-ba6a-348777645fdd)

let's execute the load query :

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/8f349267-d131-40be-b148-8f73b85551b1)

and the table is loaded !

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/350112ed-5f2a-4038-9acc-a9ffb452735b)

#### 2.3 Create Connection

In order to connect to MySQL server from Spark, we need to :

1. Identify the JDBC Connector to use
2. Add the dependency
3. Create SparkSession with database dependency
4. Read JDBC Table to PySpark Dataframe

#### 2.4 Pyspark read JDBC

A **JDBC** is a Java standard to connect to any database with the right JDBC connector jar in the classpath and provide a JDBC driver using the JDBC API. So we will be using the **pyspark.read.jdbc()** methods, to connect to the MySQL database, and reading a JDBC table to PySpark DataFrame by using PySpark with MySQL connector.

To read a table using **jdbc()** method,we will need to provide :

- MySQL server address & port
- Driver
- Database name
- Table name
- User name and Password
 
The connector **mysql-connector-java.jar** and the driver is **com.mysql.jdbc.Driver** . 

Now if we go to the Jars folder and list all the Jar files C:\..\jars>dir , we will find that we don’t have packages installed for MySQL. So we download the package from :

        https://dev.mysql.com/downloads/connector/j/
        
Ps: To check all the packages from Maven using this link :  https://mvnrepository.com/artifact/mysql/mysql-connector-java

First, open Jupyter Notebook First, with starting the Spark session, we will also download and install MySQL Packages from Maven. 

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/0a8e0aa0-aca3-4bf2-bb01-36cf178b099f)

Once the connector is downloaded, we store it in the spark folder, we may have to add the path of the **.jar** file to **CLASSPATH** and restart the laptop to avoid having this Error :
       
       py4j.protocol.Py4JJavaError: An error occurred while calling o27.load.
       : java.lang.ClassNotFoundException: com.mysql.jdbc.Driver
     
 Create the spark session and add the configuration of the connector:

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/c293a298-ef79-4e0c-a0bd-c4ed42bdfeb6)

With read() we need to provide the driver and the MySQL connection details. In the below example, I am reading a table orders from the database spark_db to the DataFrame.
 
![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/ea82d0b3-e5be-4dbf-85d5-ce798e5dfbce)

### 2.5  Write PySpark DataFrame to MySQL Database Table

Same to reading we need to provide the MySQL server & database details while writing PySpark DataFrame to the table. Use the format() to specify the driver already efined in the MySQL connector dependency. There are different modes to writing :

- **overwrite:**  drops the table if already exists by default and re-creates a new one without indexes.
- **truncate true:** to retain the index.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/88b3a31d-9d50-4b2e-ad96-43e4143c9b24)

It created a new table and loaded data into table in the database of MySQL :

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/00c7d30c-5c90-4751-aa5c-8c332b059d80)


### 3. Spark & PostgreSQL

[The PySpark code on Jupyter Notebook](https://github.com/ImaneBenHassine/Apache-Spark/blob/main/SparkETL/Spark%20%26%20PostgreSQL.ipynb)

#### 3.1 Install PostgreSQL

The interactive installer by EnterpriseDB is designed to make it quick and simple to install PostgreSQL on your computer, here is the link : https://www.enterprisedb.com/downloads/postgres-postgresql-downloads 

As show by the image below using the **pslq shell** just pressing enter,  the installation is finished plus we only have the default databases.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/15a11f2c-2510-4d13-b7c7-37e6c05233b9)

#### 3.2 Create DataBase, table

We can create the database by pgAdmin or by the Sql Shell typing : 
      
      CREATE DATABASE Spark_db ;
      
![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/de1cbccd-c17e-48f4-baa2-6910e001318a)

To access to the database : 

       \c Spark_db

Now we create the table :

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/6cee5bd8-cde4-4e8e-a2a2-c00906819e02)
       
to view the table :  

          \d  or \dt  (without the sequence)

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/0257d3b4-878f-478b-a013-ed1f5ae57b28)

the table is loaded

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/c48b7d5a-2234-4313-84a8-b0768b97737b)

We need to download the PostgreSQL JDBC Driver from Maven using this link :  https://mvnrepository.com/artifact/org.postgresql/postgresql/42.6.0

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/acb31517-bc7a-4839-afab-74e09f7e07c9)

Once we install and download Spark Packages , we create connection with PostgreSQL.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/e55a8416-ec25-474b-a08e-c5a30ba1e099)

then we read the table from PostgreSQL using Spark:

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/cfa276a2-10b2-4619-ace9-9a74340ee14a)

After we create Hive table and do transformation.
![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/bb4b2164-c5dc-4f4b-a606-ceba7c611e7c)

Finnaly we load the new table into PostgreSql from Spark using Write.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/6ece615c-d07f-4d84-a8ca-eb8c9768eff2)

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/1dc140f3-d00e-4ab2-a084-831cd3fba49c)

### 4. Spark & MongoDB

[The PySpark code on Jupyter Notebook]()

MongoDB is a **document-oriented NOSQL** database used for handling Big Data. Unlike tabular format in the conventional relational databases MongoDB uses **collections** and **documents**


#### 4.1 Install MongoDB

Download the MongoDB Community Server from this link : https://www.mongodb.com/try/download/community

As seen **"mongo"** is not recognized as internal or external an a command, so that's why we need to dowload the **shel** separately throw the link :https://www.mongodb.com/try/download/shell and extract it inside the Mongodb directory.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/7350f4cb-5e79-477f-8053-1a7214c62fee)

 Now we need to add the bin directory path of the shell to the path of the variable system, restart the command line and type :
  
      mongosh
      
 ![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/2bc1b374-8487-498f-9f9e-92386a069e79)

#### 4.2 Create DataBase, collection
#### 4.3  Write PySpark DataFrame to MongoDB Database Table

### 5. Spark & HBase
#### 5.1 Install & Setup HBase in Standalone Mode
Starting by downloading ApacheHBase following the link : https://hbase.apache.org/downloads.html

Then we need to change some settings can be found [here](https://repository.apache.org/content/repositories/releases/org/apache/hbase/hbase/2.2.6/) such as :

- file **bin/hbase.cmd** ,java_arguments and delete **%HEAP_SETTINGS%**
![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/0dfff4c8-4f85-44d8-a391-f2aa2b56c7c7)

- file **conf/hbase-env.cmd** and add variables : jdk directory , HBase repository, HeapSize , some information on the Master , Sever, Port of REGIONSERVER..
 
![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/d87293a8-fc44-4c37-8982-0e48bec31e03)

- file conf/hbase-site.xml : we add the following properties to the configuration tag :

  ![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/aaa4b9a7-56cd-458d-93ac-e27490c71d73)

- Create variable HBASE_HOME with value as the path of the Hbase directory

Now we can lunch Hbase, throw the Line command by typing 

        cd ..\HBase\hbase-2.5.5>cd bin
                bin>start-hbase.cmd

to check if all good : \bin>hbase version

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/92c06d9e-8f82-45fb-b930-ddf3701c0c89)

When I try to run **hbase shell**, I get an error message :

       This file has been superceded by packaging our ruby files into a jar and using jruby's bootstrapping to invoke them. If you need to
       source this file fo some reason it is now named 'jar-bootstrap.rb' and is located in the root of the file hbase-shell.jar and in the source tree at
       'hbase-shell/src/main/ruby'.
       
Then I discovered that it could be related to the stability of the HBase release, so I tried with an older one like **2.4.17** & **2.4.1** but still faced with the same problem. 
I discovered that the stable version is version [2.2.6](https://archive.apache.org/dist/hbase/2.2.6/), which I downloaded, and it's running smoothly.
#### 5.2 Create Table
Now we can create table and column family and insert some data :

       hbase(main):019:0> create 'table_orders', 'orders' ; 
       put 'table_orders', '1002854', 'orders:customerid', '45978'
       put 'table_orders', '1002854', 'orders:campaignid', '2141'
       put 'table_orders', '1002854', 'orders:orderdate', '2009-10-13 00:00:00'
       put 'table_orders', '1002854', 'orders:city', 'NEWTON'
       put 'table_orders', '1002854', 'orders:state', 'MA'

       put 'table_orders', '1002855', 'orders:customerid', '125381'
       put 'table_orders', '1002855', 'orders:campaignid', '2173'
       put 'table_orders', '1002855', 'orders:orderdate', '2009-10-13 00:00:00'
       put 'table_orders', '1002855', 'orders:city', 'NEW ROCHELLE'
       put 'table_orders', '1002855', 'orders:state', 'NY'

       put 'table_orders', '1002856', 'orders:customerid', '103122'
       put 'table_orders', '1002856', 'orders:campaignid', '2141'
       put 'table_orders', '1002856', 'orders:orderdate', '2011-06-02 00:00:00'
       put 'table_orders', '1002856', 'orders:city', 'MIAMI'
       put 'table_orders', '1002856', 'orders:state', 'FL'

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/ff6f929a-be91-4c97-9e52-d729d527a56f)

#### 5.3 Create Connection
To develop applications HBase with Spark, we need to import a dependency from the Maven repository such as :
 - Apache HBase Client
 - Apache HBase Spark Connector
 - shc-core : was not availbale on Maven but can be downloaded from this [link](https://repo.hortonworks.com/#browse/search=keyword%3Dshc-core%20AND%20version%3D1.1.0.2.6.2.1-1:NX.coreui.model.Component-601:d9027a70) , plus we need to add it to the project Externel Libraries

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/b17e42f1-a255-4e95-9857-336684a74efb)

Now we can start our application by creating the class object for integrating Spark & HBase.

First we create the catalog :

 ![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/d9b0e617-506e-4f6e-8e92-6a52d14dd1b7)

Then we establish the connection with HBase, as we have imported with the connector data sources that we just use **sessionspark.read** and read it as a datasource like any other datasources

