# Table of Contents 
1. [Introduction](README.md#introduction)
2. [Pipeline](README.md#pipeline)
3. [Requirements](README.md#requirements)
4. [Environment Set Up](README.md#Environment%20Setup)
5. [Repository Structure and Run Instructions](README.md#Repository%20Structure%20and%20Run%20Instructions)


# Introduction
**Trends in pricing and a measure of online inflation: Visualizing history of online price inflation using the Common Crawl**

This is a project during the Insight Data Engineering Program (New York, 20B Session). The goal of this project is to scrape product information, descriptions, 
categories and subcategories, storing the data in an appropriate (relational or NOSql) database for easy retrieval and aggregation. The program uses 
a subset (~XTB) of the [Common Crawl](https://commoncrawl.org/), an archive of web page content. The results can be used to help companies measure online price 
inflation for different products and set plans for the future accordingly. A sample batch job has been executed with a set of database names and the UI with 
the results is temporarily displayed at [visual](somevisualization). A recording of the WebUI is also available [here](X). 

# Pipeline
The data was ingested from Amazon S3 and Apache Airflow was used for scheduling the batch processing tasks corresponding to each month of the Common Crawl data. 
Then Apache Spark is used to process and aggregate the data and finally Apache Zeppelin is used to gain insights from the processed data in a user-friendly way.

# Requirements

Languages 
* Python 3.7
* Scala 2.11.8

Technologies
* Spark
* Airflow 
* Zeppelin

Third-Party Libraries
* AWS CLI

# Environment Setup
Install and configure [AWS](https://aws.amazon.com/cli/) and the open-source tool and setup an AWS EMR cluster. At launch time, emr-5.30.0 version was used.
It comes with Python version 3.7.6, pyspark version 2.4.5-amzn-0 and Zeppelin 0.8.2.


**AWS EMR Clusters Setup**

Currently using only one master node and one core node (can be scaled up)

# Repository Structure and Run Instructions


`./airflow/` will contain the scheduling scripts.

`./project/` will contain all configuration files and scripts for the project

`./frontendapp/` will contain zeppelin notebooks to visualize results and trends in pricing from the s3,airflow,spark pipeline




