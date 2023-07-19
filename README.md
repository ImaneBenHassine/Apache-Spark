# Apache-Spark

## Introduction

This repository contains my notes of my Apache Spark learning journey through code and examples to show how pyspark can be used to explore big data.

**Apache Spark** is an Open source framework, an in-memory computing processing engine that processes data on the Hadoop Ecosystem. It processes both **batch** and **real-time** data in a **parallel** and **distributed** manner.

**Spark VS MapReduce** : 

- Spark is a lighting-fast in-memory computing process engine, 100 times faster than MapReduce, 10 times faster to disk but MapReduce is I/O intensive read from and writes to disk.
- Spark supports languages like Scala, Python, R, and Java, unlike Hadoodp which is written in java only.
- Spark Processes both batch as well as Real-Time data when Hadoop is batch processing.

**Components/modules of Apache Spark** : Apache Spark comes with 
- SparkCore
- Spark SQL
- Spark Streaming
- Spark MlLib
- GraphX

**Installation**: Spark can be installed in 3 different ways.

- Standalone mode
- Pseudo-distribution mode
- Multi cluster mode
  
**PySpark** is a Spark library written in Python to run Python applications using Apache Spark capabilities. Using PySpark we can run applications parallelly on the distributed cluster (multiple nodes).

In other words, PySpark is a **Python API** for Apache Spark which is an analytical processing engine for large scale powerful distributed data processing and machine learning applications.

PySpark supports two types of Data Abstractions:

  1. ***RDDs*** (Resilient Distributed Datasets)
  2. ***DataFrames*** 

## Steps 

   1. Installing Spark
   2. Installing & configurating Intellij
   3. Debugging Errors with Scala 
   4. API logging   
   
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

#### 2.4 Setup Scala SDK
Now we need to intsall Scala SDK by right click on the project then **Add Framework Suport**

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/45cdb426-1668-4f3c-84e7-bca7cbccce5b)

As we can see we can't find **Scala**  in the list of the desired technologies so we need to add Scala SDK as a Global library for the project.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/dd30a950-0f2f-48cf-b580-829abcb0ba4a)

And now we can add Scala as a framework support.

#### 2.5 Changes to pom.xml file

We start by adjusting the Scala version to the latest version, mine is 2.12.17

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/b536327f-9f0a-47f4-bf30-9fd65cd6cee6)

To solve this error  :

             [WARNING] Error injecting: org.apache.maven.report.projectinfo.CiManagementReport java.lang.NoClassDefFoundError: 

we may need to define the maven-site-plugin and the maven-project-info-reports-plugin along with the version numbers in the pom.

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/0cb58d86-21e4-41b1-88c0-427637ee769d)

#### 2.6 Add Spark Dependencies to Maven pom.xml File
            
We need to add Spark dependencies to pom.xml file such as :

- **Core libraries** for Apache Spark from https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.13/3.4.0
- **Spark Project SQL** to work with structured data based on DataFrames via : https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.13/3.4.0.
- **Spark Project Streaming** from https://mvnrepository.com/artifact/org.apache.spark/spark-streaming_2.13/3.4.0

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/7cc495e1-0cf8-4728-8446-e17c195c9c02)

Sometimes, we may need to re-load the dependencies or restart the IntelliJ because it's not automatically loaded . Finally, we can see BUILD SUCCESS as below by selecting Maven from the right top corner, then clean install:

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/478b1519-568a-4536-9881-15b347945cdd)

Or we can add the option to automatically download all the dependecies in the **Settings** :

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/cbbb055a-0160-41a9-b80b-96f792cb19eb)

**Note** : we may need to change the JDK to Java 8 instead of the 20 installed previously. Now let's run our first program

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/8945a7e6-e3c9-485b-81bb-e7ce91e84c61)

#### 2.7 Deployment with Maven

Scala Library » 2.12.17 : Standard library for the Scala Programming Language

Maven Scala Plugin : is used for compiling/testing/running/documenting scala code in maven.

### 3. Debugging Errors with Scala 

- when submiting the project it gives below error :

          Caused by: java.lang.ClassNotFoundException: org.apache.spark.sql.SparkSession
  
  fixed by adding "Include dependencies with Provided scope" to the Run/Debug Configuration :
  
  ![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/ac58fda7-26d5-40f2-8482-d4241a76566e)

- when saving an RDD to a file : 

        Caused by: java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Ljava/lang/String;I)Z

I solved by adding under hadoop/bin the **hadoop.dll** from https://github.com/steveloughran/winutils/blob/master/hadoop-3.0.0/bin/hadoop.dll 

- When reading from a SQL Server database, unable to find valid certification path to requested target
  
        Caused by: sun.security.validator.ValidatorException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid 
        certification path to requested target

fixed by adding **"trustServerCertificate=true";** to the Url of the conncetion since we use windows authentifecation for **SQLServer**.

- Here we need to download the JDBC driver which can be added to a Maven project by adding it as a dependency in the POM.xml file with the following link : 

  https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc.

  - This error come up when trying to install the last **SQLServerDriver**
 
         com/microsoft/sqlserver/jdbc/SQLServerDriver has been compiled by a more recent version of the Java Runtime (class file version 55.0), this version of the Java 
         Runtime only recognizes class file versions up to 52.0

So i had to try other version and found out that **Microsoft JDBC Driver 6.0 for SQL Server** is the most recommanded and did worked with the **mssql-JDBC**  : https://www.microsoft.com/en-us/download/details.aspx?id=11774

Now we run sqljdbc_<version>_enu.exe, then copy the ddl file under pom directory we need to add it as a standard library of the Global Libraries of the project 

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/a0584d05-3fd9-49fd-a3e4-91944302ee04)

- So this error pomp out

       sql server - no mssql-jdbc_auth-8.2.1.x64 in java.library.path
 
 fixed by adding the **sqljdbc_auth.dll** to %JAVA_HOME%/jre/bin and the connection was finally established!

### 4. API logging
If we ask an expert developer about the most annoying thing about an application, the answer might be related to **logging**. If there is no suitable logging in an application, **maintenance** will be a **nightmare**. Most of the application go through Development testing, unit testing, integration testing. But when it comes to **production**, you will always face **unique scenarios and exception**. 

So the only way to figure out what happened in a specific case is to **debug through the logs** and using Logging API in application isn’t a luxury, it’s a must have. Many frameworks provide some way of default logging, but it’s always best to go with the industry standard logging mechanism. 

Like **Apache Log4j** is one of the most widely used logging frameworks and is an open source library that’s published and licensed under Apache Software and Apache Log4j 2 is the next version, that is far better than Log4j.

We can debug an application using Eclipse Debugging or some other tools, but that is not sufficient and feasible in a production environment. Logging mechanism will provide several benefits with less maintenance cost that we will not find in normal debugging such as :
- No need for human intervention
- Can be integrated with persistent storage (Files, Database, NoSQL database, etc.)
- Can be used for achieving auditing if it’s used efficiently
- Sufficient
- More productive

In this Log4j2 section, we will learn how to get started with Apache Log4j2. We will also explore Log4j2 architecture, log4j2 configuration, log4j2 logging levels, appenders, filters and much more.

#### 4.1 Log4j2 architecture
- Applications will ask **LogManager** for a **Logger** with a specific name.
- **LogManager** will locate the appropriate **LoggerContext** and then obtain **Logger** from it.
- If the **Logger** isn’t created yet, it will be created and associated with **LoggerConfig** according to three choices below either Logger instance will be created and 
       associated with the **LoggerConfig** that have the **same name**. Or Logger instance will be created and associated with the LoggerConfig that have the same **Loggers 
       parentpackage**. Or Logger instance will be created and associated with the **Root LoggerConfig**. 
- **LoggerConfig** objects are created from Logger declaration in the configuration file. LoggerConfig is also used to handle **LogEvents** and delegate them for their 
       defined **Log4j2 Appenders**.
- Root logger is an exceptional case, in terms of its existence. It always exists and at the top of any logger hierarchy.
- The name of log4j2 loggers are case sensitive.
- Except root logger, all loggers can be obtained through passing their name into **LogManager.getLogger()**.
- LoggerContext is a vocal point for Logging system as you may have multiple LoggerContexts inside your application. Per each LoggerContext an active configuration should be 
       set.
- Log4j2 configuration contains all Logging system assets; LoggerConfig(s), Appender(s), Filter(s) and many others.
- Calling of LogManager.getLogger() by passing the same name will always return the reference for the exact same logger instance.
- Configuration of Logging system is typically done with the application initialization. This can take different forms; programmatically or by reading a log4j2 configuration 
       file
  
#### 4.2 log4j2.properties
The log4j2.properties file is a log4j configuration file which keeps properties in key-value pairs. By default, the LogManager looks for a file named log4j.properties in the CLASSPATH.

Configuration of Log4j 2 can be accomplished in 1 of 4 ways: Through a configuration file written in **XML**, JSON, YAML, or properties format.

- The level of the root logger is defined as DEBUG. The DEBUG attaches the appender named X to it.
- Set the appender named X to be a valid appender.
- Set the layout for the appender X.

# Define the root logger with appender X
log4j.rootLogger = DEBUG, X

# Set the appender named X to be a File appender
log4j.appender.X=org.apache.log4j.FileAppender

# Define the layout for X appender
log4j.appender.X.layout=org.apache.log4j.PatternLayout
log4j.appender.X.layout.conversionPattern=%m%n


#### 4.2 Get Log4j
Declares the following dependencies from Maven
- Apache Log4j API
- Apache Log4j Core

#### 4.3 log4j.properties
Create a log4j.properties file and put it into the resources folder

we started by importing dependecies :

         import org.apache.log4j._

then we create a logging for our class with a specific name.

        private val log_appli : Logger =LogManager.getLogger("Logger_Console")

 And we add logging message on one of our functions as shwon below
 
           /* my first function  */
       def count_leng(text: String) : Int = {
       log_appli.info("start the  logging")
       log_appli.info(s"the parameter logged by Log4J for this function is : $text")
       log_appli.info(s"Message warning Log4J : ${10+15}") // calculation done inside sting

we run the code , but this error appear :

  log4j:WARN No appenders could be found for logger (Logger_Console).
  log4j:WARN Please initialize the log4j system properly.
  log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
 
at this point we need to add the file configuration into the project, under ressources as an EXML file
 
![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/370700a9-88e6-4989-b3dc-1552eb7201df)

