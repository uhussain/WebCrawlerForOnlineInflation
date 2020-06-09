import boto3
"""
This script takes every 100 lines of the AWS file and outputs into local drive.
"""

s3 = boto3.resource('s3')
obj = s3.Object("athena-east-2-usama", "Unsaved/2020/06/09/112be0b3-7589-4b70-a240-10d781c28b60.csv")
lines = obj.get()['Body'].read().split()
spacing=100


with open('new.csv', 'w') as file:
    count=0
    for row in lines:
        if count%100 == 0:
            file.write(str(row)+"\n")
        count += 1
