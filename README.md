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
Run AWS Athena queries as listed in step 3) below on each month of the Common Crawl data in S3 and save output in the form of csv file in s3 bucket for each month. 
Then Apache Spark will be used to process each month data using the csv file output from Athena to collect product information for each month of crawl
data (hence collecting product information over time) in S3. Apache Zeppelin/Suitable visualization tool will then be used to potentially track products and prices 
across different months from the last step.

# Requirements

1)  S3:  Set up and S3 bucket.  In this case:  s3://athena-east-2-usama/
2)  Athena:  Open Athena on AWS.  Follow the instructions to set up "Running SQL Queries with Athena" here:  https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/
3)  Run Athena with the example in https://github.com/uhussain/WebCrawlerForOnlineInflation/tree/development/athena/athena_instructions.txt
4)  Start EMR in Amazon with Spark and Hadoop.  SSH in.
5)  Add the following to ~/.bashrc and source ~/.bashrc:
export SPARK_HOME=/usr/lib/spark
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH  
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
6)  Install the following on EMR (both master and workers) with "pip install --user":  warcio, boto3, bs4, nltk 
7)  Execute main.py in master node.

Languages 
* Bash
* Python 3.7
* Scala 2.11.8

Technologies
* Spark
* AWS Athena
* Zeppelin/Visualization tool
* Amazon DynamoDB

Third-Party Libraries
* AWS CLI

# Environment Setup
Install and configure [AWS](https://aws.amazon.com/cli/) and the open-source tool and setup an AWS EMR cluster. At launch time, emr-5.30.0 version was used.
It comes with Python version 3.7.6, pyspark version 2.4.5-amzn-0 and Zeppelin 0.8.2.


**AWS EMR Clusters Setup**

Currently using only one master node and one core node (can be scaled up)

# Repository Structure and Run Instructions


`./athena/` contains instructions on how to query common crawl data with AWS Athena .

`./prototype/` contains all python configuration files and scripts for running the project locally and output product info to a local amazon dynamoDB table

`./spark/` contains the main.py spark script to launch spark jobs using the output from athena query

`./frontendapp/` will contain zeppelin notebooks to visualize results and trends in pricing from output tables saved in s3




