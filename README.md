# Apache-Spark

## Introduction

This repository contains my notes of my Apache Spark learning journey through code and examples to show how pyspark can be used to explore big data.

**PySpark** is a Spark library written in Python to run Python applications using Apache Spark capabilities. Using PySpark we can run applications parallelly on the distributed cluster (multiple nodes).

In other words, PySpark is a **Python API** for Apache Spark which is an analytical processing engine for large scale powerful distributed data processing and machine learning applications.

PySpark supports two types of Data Abstractions:

  1. ***RDDs*** (Resilient Distributed Datasets)
  2. ***DataFrames*** 

## Steps
### 1. Installing Spark on Windows 10
   #### 1.1 Install Java 
  Download the latest Java version from the link : https://www.oracle.com/java/technologies/downloads/#jdk20-windows
   
    a. Set environmental variables:
   
         i. User variable:
              - Variable: JAVA_HOME
              - Value: C:\Program Files\Java\jdk
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
Open a new command-prompt window using the right-click and Run as administrator, go to spark directory " cd C:\..\bin " then execute : **spark-shell.cmd**

If the environment path was correctly setted, the system should display several lines indicating the status of the application. You may get a Java pop-up. Select Allow access to continue.

Finally, the Spark logo appears, and the prompt displays the Scala shell !

![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/9b882c24-d4be-4ef4-9d41-3023330e0bed)


### Spark & MySQL
Download the Community (GPL) version of MySQL for Windows from the link : https://dev.mysql.com/downloads/file/?id=518835

Then create the "root" account while installing to test the first conncetion !
![image](https://github.com/ImaneBenHassine/Apache-Spark/assets/26963240/e44d085e-e1c2-4289-b733-37d07b5e1fd7)

Now we need to creta a "user" account so we can have acces to the database


