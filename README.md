# Apache Hadoop

[![Java](https://img.shields.io/badge/java-yellow?style=for-the-badge&logo=openjdk&logoColor=white)](https://dev.java/)
[![Hadoop](https://img.shields.io/badge/Hadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black)](https://hadoop.apache.org/)
[![Apache](https://img.shields.io/badge/apache-red?style=for-the-badge&logo=apache)](https://httpd.apache.org/)
[![Linux](https://img.shields.io/badge/linux-black?style=for-the-badge&logo=linux)](https://www.linux.org/)
[![Shell Script](https://img.shields.io/badge/shell_script-%23121011.svg?style=for-the-badge&logo=gnu-bash&logoColor=white)](https://www.shellscript.sh/)
[![Mapreduce](https://img.shields.io/badge/mapreduce-yellow?style=for-the-badge)](https://www.databricks.com/glossary/mapreduce)
![NetBeans IDE](https://img.shields.io/badge/NetBeansIDE-1B6AC6.svg?style=for-the-badge&logo=apache-netbeans-ide&logoColor=white)

# Overview
This repository contains a series of programming assignments implemented using Apache Hadoop, focusing on MapReduce functionalities. The objectives are designed to demonstrate the capabilities of Hadoop in processing large datasets, optimizing runtime performance, and implementing custom data types for MapReduce jobs.
> **_NOTE:_** Please visit ProjectGoals.png image to get the detailed objectives for this project

![Alt text](Image.png)

# Contents
- **access.log:** The log file used in Part 2 of the assignments.
- MapReduce code files for each part of the assignment.
- Additional resources and datasets as required for the assignments.

# Project's Overview
## Part 2 – IP Access Count
**Objective:** Count the number of times each IP accessed a website using the access.log file in HDFS.

**Implementation:**
- Copy access.log to HDFS under /logs directory.
- Run MapReduce job to count IP accesses.
- Measure runtime without a Combiner.
- Add a Combiner (using the Reducer code) to optimize performance and measure runtime.

## Part 3 – NYSE Data Analysis
**Objective:** Analyze the NYSE dataset to find the maximum stock price.

### Implementation:
**3.1:**
- Copy NYSE dataset (DailyPrices_A to DailyPrices_Z) to HDFS without merging.
- Implement MapReduce to find the max stock_price_high for each stock.
- Capture running time.

**3.2:**
- Merge NYSE files into a single file in HDFS.
- Repeat the analysis on the merged file.
- Compare running times.

## Part 4 – Exploring FileInputFormat Classes for MapReduce
**Objective:** Implement MapReduce programs using different FileInputFormat classes.

**Implementation:**
- Use classes like CombineFileInputFormat, FixedLengthInputFormat, etc.
- Apply any analysis (e.g., counting) on chosen input files.
- Focus on understanding the usage of each FileInputFormat class.

## Part 5 – Custom Writable Object
**Objective:** Create a Writable object to store specific fields from the NYSE dataset.

**Implementation:**
- Develop a custom writable class with fields for max/min stock volume and max stock_price_adj_close.
- Use this writable object in both Mapper and Reducer.

## Part 6 – Optimization Using Text Object
**Objective:** Optimize Part 5 using a Text object with delimiters and a Combiner.

**Implementation:**
- Store multiple values in a Text object with delimiters.
- Use a Combiner to optimize performance.
- Compare runtime with Part 5.

# Running the Project
- Follow the standard Hadoop setup and execution procedures for each MapReduce job.
- Ensure all datasets are correctly placed in HDFS as required.
- Compile and package the code as per Hadoop's requirements.

# Dependencies
- Apache Hadoop (version specified in the project)
- Java SDK (compatible version with Hadoop)

# License
This project is licensed under the MIT License.