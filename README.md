# MapReduce Temperature Analysis

## Overview
This project implements a MapReduce job in Java to calculate the average daily maximum and minimum temperatures from climate data using Hadoop.

## Prerequisites
- Apache Hadoop 3.2.1
- Java 8

## Components
- **TemperatureMapper**: Filters records by the specified month and emits the date along with max and min temperatures.
- **TemperatureReducer**: Calculates the daily average of maximum and minimum temperatures.
- **Controller**: Sets up and executes the MapReduce job.

## Usage
Compile the Java classes and package into a JAR. 
```bash
mvn install
```

Run the job with Hadoop:
```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/MinMaxAvgOfTemperatureInHadoop-0.0.1-SNAPSHOT.jar com.example.controller.Controller 202401 input/avg/2024.csv output/avg
```


## Detailed Steps
copying jar & dataset to docker node
```bash
docker cp MinMaxAvgOfTemperatureInHadoop-0.0.1-SNAPSHOT.jar resourcemanager:/tmp
docker cp 2024.csv resourcemanager:/tmp
```
connect to docker
```
docker exec -it resourcemanager /bin/bash
```
copy the jar from tmp location to a location where all existing jobs are already present.
```bash
cp /tmp/MinMaxAvgOfDatasetinHadoop-0.0.1-SNAPSHOT.jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```
copying dataset from linux filesystem to hadoop file system.
```bash
hadoop fs -mkdir -p input/avg
hadoop fs -put /tmp/2024.csv input/avg
```

Executing job with input type of yyyyMM 
``` bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/MinMaxAvgOfTemperatureInHadoop-0.0.1-SNAPSHOT.jar com.example.controller.Controller 202401 input/avg/2024.csv output/avg
```

To view output 
``` bash
hadoop fs -cat output/avg/p*
```
To view logs of previous instances
``` bash
hadoop fs -cat /app-logs/root/logs-tfile/application_1707202062990_0002/879565fd64a6_44807
```

To reexecute the same jar with different datasets then you have to delete previous output data
``` bash
hadoop fs -rm -R output/avg
```




