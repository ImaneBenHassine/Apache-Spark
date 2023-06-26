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
   3. PySpark & MySQL
   4. PySpark & PostgreSQL
   5. PySpark & MongoDB
   
   
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

Scala Library » 2.12.17
Standard library for the Scala Programming Language

Maven Scala Plugin
The maven-scala-plugin is used for compiling/testing/running/documenting scala code in maven.




