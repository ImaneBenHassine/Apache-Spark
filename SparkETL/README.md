### 1. Spark & SQL
[The Scala code on IntelliJ]()

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
[The Scala code on IntelliJ]()

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
 - shc-core : was not availbale on Maven but can be downloaded from this [link](https://mvnrepository.com/artifact/com.hortonworks/shc-core/1.1.1-2.1-s_2.11), plus we need to add it to the project Externel Libraries

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/78360121-4898-4bb3-921b-372992e2f508)

Now we can start our application by creating the class object for integrating Spark & HBase.

First we create the catalog :

 ![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/d9b0e617-506e-4f6e-8e92-6a52d14dd1b7)

Then we establish the connection with HBase, as we have imported with the connector data sources that we just use **sessionspark.read** and read it as a datasource like any other datasources.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/4f86d1db-d361-48a2-b39a-8fb0e4c7f016)

### 6. Spark & Cassandra
[The Scala code on IntelliJ]()
#### 6.1 Install & Setup Cassandra in Standalone Mode
Starting by downloading a stable version following the link : https://cassandra.apache.org/download/.

Then creating an environment variable pointing to the root folder of cassandra installation : **CASSANDRA_HOME**. After we install the **Thrift Server**. needed for Cassandra from [here](https://thrift.apache.org/download). We also need to install **Python** preferably [V2.7](https://www.python.org/downloads/release/python-2718/) 

Now we can lunch Cassandra by running the script on CMD as admist

        /../bin>cassandra.bat -f

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/95b51cc1-374d-4cab-b2e1-c390d7f6a58f)

as seen startup complete & Thrift server is enabled so we can lunch now the **cqlch** to execute SQL query on Cassandra

          bin>cqlsh.bat

 ![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/d6ab0c7c-dfba-4981-ba0a-083929d89359)

#### 6.2 Create Table
To create the table we use **Cassandra Query Language (CQL)**. CQL offers a model similar to SQL where the data is stored in tables containing rows of columns
whose schema defines the layout of the data in the table. 

Tables are located in **keyspaces**. A keyspace defines options that apply to all the keyspace’s tables where we set the replication strategy and the replication factor. A good general rule is one keyspace per application.A table is always part of a keyspace.

- **SimpleStrategy** that defines a replication factor for data to be spread **across the entire cluster**. This is generally not a wise choice for production, as it does not respect datacenter layouts and can lead to wildly varying query latency. SimpleStrategy supports a single mandatory argument:**replication_factor** whiche describe the number of replicas to store per range.
- **NetworkTopologyStrategy** is a production-ready replication strategy that sets the replication factor **independently for each data-center**.

        cqlsh> CREATE KEYSPACE demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

        cqlsh> CREATE TABLE IF NOT EXISTS demo.spacecraft_journey_catalog (  spacecraft_name text,  journey_id timeuuid,  start timestamp,  end timestamp,  active boolean,  
                  summary text,  PRIMARY KEY ((spacecraft_name), journey_id)) WITH CLUSTERING ORDER BY (journey_id desc);

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/df6ed3b2-02b9-4d72-b294-ad28061b03b7)

To insert data 

           cqlsh> INSERT INTO demo.spacecraft_journey_catalog (spacecraft_name, journey_id, start, end, active, summary) VALUES ('vostok1', 805b1a00-5673-11a8-8080- 
                 808080808080, '1961-4-12T06:07:00+0000', '1961-4-12T07:55:00+0000', False, 'First manned spaceflight. Completed one Earth orbit.');

Using the **shell** to create tables , insert rows could be quite challenging. That's why it's recommended to use DataStax.

#### 6.3 Using DataStax
We can download [DataStax Enterprise](https://downloads.datastax.com/#studio) which is a scale-out data infrastructure built on the foundation of Apache Cassandra.  

           /../datastax-studio-6.8.26\bin>server.bat

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/7d740a79-b88d-479c-83d9-19ec7efe0e61)

 Studio will be running at [localhost](http://localhost:9091/). Then we choose **Working with CQL v6.8.0** and a new Notebook will be created.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/b75a2f3f-eb27-4bfc-9aae-ef419b33aacb)

Ps: Studio only supports DataStax Astra & Enterprise clusters so it won't work with the community version of Cassandra.

#### 6.4 Create Connection

If query is known, there's no need to use Spark to question Cassandra. This is only useful when the requests are not known beforehand or when cassandra's processing of difficult queries such as aggregation or groupby that negatively impact its performance.
- 1 Spark executor = 1 Cassandra node.
- the connection is made via the **Java Driver Datastax** connector, choosing the stablest version on Maven Repository **Spark Cassandra Connector 2.4.2**
- supports features of the Datastax Spark Connector:  for **RDD API** (cassandraTable, saveToCassandra, repartitionByCassandraTable, joinWithCassandraTable) for **DataFrame API** (Datasource).

Now we can integrate database :

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/fc799e7d-4618-4931-b565-3881bd588e40)

### 7. Spark & Elasticsearch
Elasticsearch is the distributed search and analytics engine , provides near real-time search and analytics for all types of data : structured or unstructured text, numerical data, or geospatial data, Elasticsearch can efficiently store and index it in a way that supports fast searches.

[The Scala code on IntelliJ]()

#### 7.1 Install & Setup Elasticsearch 

Starting by downloading Elasticsearch following this [link](https://www.elastic.co/guide/en/elasticsearch/reference/7.11/windows.html) using the .msi package

After installation, when trying to lunch elasticsearch throw the web **http://localhost:9200/** or the shell this error appears:

                  ELK\7.11.2\bin>elasticsearch.exe
                  Erreur : Unable to find or load the Warning main class

It turns out that elestecsearch has its own **jdk**, but the Java installed on my system, which runs all other applications is not identical to the one that comes with elestecsearch.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/a0e4f99c-e8a5-4e20-a50e-2c2e83a4db7b)

So we should reinstall an elasticsearch version that has the same Java version(1.8 in my cass) or delete the variable JAVA_HOME so elasticsearch can point directly to it's own jdk.

Now it works :
![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/db1a0b2b-9392-408a-9077-2944b92355ff)

and the web shows all details about cluster, version..

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/305f021d-46e7-4aab-ba45-dc5859d8b559)

Now let's download **Kibana** to be able to visualize the data and navigate the Stack folowing this  [link](https://www.elastic.co/downloads/kibana). Then we nedd to set some changes in the **kibana.yml** configuration such as :

          server.port: 5601
          server.host: "localhost"
          elasticsearch.hosts: ["http://localhost:9200"]

We are good to run Kibana :

                 \bin>kibana.bat
                 
Kibana server start running at http://localhost:5601. Now we can create indexes add it and visual it with Kibana.

To develop applications Elasticsearch with Spark, we need to import a dependency from the Maven repository such as :
 - ElasticSearch Spark Connector
 - Client REST HTTP

#### 7.2 Create Index
To create connection with ElasticSearch we will need to specify:
- format : org.elasticsearch.spark.sql
- es.port : 9200
- es.nodes : localhost
- es.nodes.wan.only: true if data comes from Cloud
- name of the index and the type : indexe/doc

it will append or modify if already index exists.

    import org.elasticsearch.spark.sql._
    df_orders.write
      .format("org.elasticsearch.spark.sql")
      .option("es.port","9200")
      .option("es.nodes","localhost")
      .save("index_ibh/doc")
   
![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/f92e29e1-0466-49ea-b6cb-bcb10b8267c8)

Here we can specify wich column we need it to be searchabla or not

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/bcd0e164-4e14-4bbd-8155-f34a4855e1a8)

Later, we can use Kibana to play data, searching for a specific value, create dashboards..

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/17e4f321-892d-4bf9-b300-ca67ab518242)









