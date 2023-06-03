# Apache-Spark

## Introduction

This repository contains my notes of my Apache Spark learning journey through code and examples to show how pyspark can be used to explore big data.

**PySpark** is a Spark library written in Python to run Python applications using Apache Spark capabilities. Using PySpark we can run applications parallelly on the distributed cluster (multiple nodes).

In other words, PySpark is a **Python API** for Apache Spark which is an analytical processing engine for large scale powerful distributed data processing and machine learning applications.

PySpark supports two types of Data Abstractions:

  1. ***RDDs*** (Resilient Distributed Datasets)
  2. ***DataFrames*** 

## Steps:
#### 1. Installing Spark on Windows 10
   #### 1.1 Install Java 8 
  Download Java 8 from the link : http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html
   
    a. Set environmental variables:
   
         i. User variable:
              - Variable: JAVA_HOME
              - Value: C:\Program Files\Java\jdk1.8.0_91
         ii. System variable:
              - Variable: PATH
              - Value: C:\Program Files\Java\jdk1.8.0_91\bin
     
   #### 1.2 Install Spark 
   Download spark-3.4.0-bin-hadoop3 from the following link: http://spark.apache.org/downloads.html and extract it.
   
    a. Set environmental variables:
   
         i. User variable:
              - Variable: SPARK_HOME
              - Value: C:\..\spark\spark-3.4.0-bin-hadoop3

         ii. System variable:
              - Variable: PATH
              - Value: C:\..\spark\spark-3.4.0-bin-hadoop3\bin
              
   #### 1.3 Windows Utilities
   Download it from the link: https://github.com/steveloughran/winutils/blob/master/hadoop-3.0.0/bin/winutils.exe
    
    a. Set environmental variables:
   
         i. User variable:
              - Variable: HADOOP_HOME
              - Value: C:\..\spark\Hadoop

         ii. System variable:
              - Variable: PATH
              - Value: C:\..\spark\Hadoop\bin
              
Execute Spark on cmd : spark-shell
