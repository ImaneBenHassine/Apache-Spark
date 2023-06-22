# Apache-Spark

## Introduction

This repository contains my notes of my Apache Spark learning journey through code and examples to show how pyspark can be used to explore big data.

**PySpark** is a Spark library written in Python to run Python applications using Apache Spark capabilities. Using PySpark we can run applications parallelly on the distributed cluster (multiple nodes).

In other words, PySpark is a **Python API** for Apache Spark which is an analytical processing engine for large scale powerful distributed data processing and machine learning applications.

PySpark supports two types of Data Abstractions:

  1. ***RDDs*** (Resilient Distributed Datasets)
  2. ***DataFrames*** 

## Steps 

   1. Installing Spark
   2. Installing & configurating Intellij
   3. Spark & MySQL
   4. Spark & PostgreSQL
   5. Spark & MongoDB
   
   
### 1. Installing Spark on Windows 10
   #### 1.1 Install Java 
  Download the latest Java version from the link : https://www.oracle.com/java/technologies/downloads/#jdk20-windows
   
    a. Set environmental variables:
   
         i. User variable:
              - Variable: JAVA_HOME
              - Value: C:\Java\
         ii. System variable:
              - Variable: PATH
              - Value: %JAVA_HOME%\bin
     
   ![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/23de67b8-b6c7-41ba-bcba-931b0355f9ef)
  
   #### 1.2 Install Spark 
   Download spark-3.4.0-bin-hadoop3 from the following link: http://spark.apache.org/downloads.html and extract it.
   
    a. Set environmental variables:
   
         i. User variable:
              - Variable: SPARK_HOME
              - Value: C:\..\spark\spark-3.4.0-bin-hadoop3

         ii. System variable:
              - Variable: PATH
              - Value: %SPARK_HOME%\bin
              
   #### 1.3 Windows Utilities
   Download it from the link: https://github.com/steveloughran/winutils/blob/master/hadoop-3.0.0/bin/winutils.exe
    
    a. Set environmental variables:
   
         i. User variable:
              - Variable: HADOOP_HOME
              - Value: C:\..\spark\Hadoop

         ii. System variable:
              - Variable: PATH
              - Value: %HADOOP_HOME%\bin
 
#### 1.4 Launch Spark
Open a new command-prompt window using the right-click and Run as administrator, go to spark directory " cd C:\..\bin " then execute : 
    
       spark-shell.cmd

Or using **spark-submit.cmd** , adapted more for the production deployment

If the environment path was correctly setted, the system should display several lines indicating the status of the application. You may get a Java pop-up. Select Allow access to continue.

Finally, the Spark logo appears, and the prompt displays the Scala shell !

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/9b882c24-d4be-4ef4-9d41-3023330e0bed)

Open a web browser and navigate to http://desktop-o58pauc:4040
![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/36276ae9-00ff-4371-a77f-94565dae6f18)

#### 1.5 Test Spark

Let's use Scala to read the contents of a file such as the README file in the Spark directory.
![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/4f0fbb8a-ef5f-4674-a47b-288e615bffe9)

Then, we can view the file contents by using this command to call an action which instructs Spark to print 11 lines from the file you specified 

       r.take(11).foreach(println)

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/c29ae7f4-53ce-4be4-95cf-39367d02e877)

### 2. Installing & configurating Intellij

IntelliJ IDEA is the most used IDE to run Spark applications written in Scala due to its good Scala code completion.

#### 2.1 Setup IntelliJ IDEA for Spark

We can download the community edition **IntelliJ IDEA community** following this link : https://www.jetbrains.com/idea/download/?section=windows#section=windows

#### 2.2 Install Scala Plugin

From the Plugins option from the left panel we install  the **Scala** plugin and then restart the IntelliJ IDE.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/9d56a3bb-dba4-4c30-b838-1ee1eb95952f)

#### 2.3 Create a Scala project In IntelliJ

We select a New Project from **Maven** generators then we select **org.scala-tools.archetypes:scala-archetypes-simple** as the archetype which is a template that creates the right directory structure and downloads the required default dependencies. Since we have selected Scala archetypes, it downloads all Scala dependencies and enables IntelliJ to write Scala code. Then we select our JDK since Scala is a JVM language so it will need it to run ,to compile & execute..

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/207a8e9b-ec8a-4a44-99f5-60a13bd1987e)

So this creates a project on IntelliJ and if we expand the project we can see App.scala file.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/ea1c1762-86ef-434b-b248-1dbb8b9ad8bc)

#### 2.3 Setup Scala SDK
Now we need to intsall Scala SDK by right click on the project then **Add Framework Suport**

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/45cdb426-1668-4f3c-84e7-bca7cbccce5b)

As we can see we can't find **Scala**  in the list of the desired technologies so we need to add Scala SDK as a Global library for the project.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/817ff652-c673-49a2-9f0a-114fc2648488)








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






