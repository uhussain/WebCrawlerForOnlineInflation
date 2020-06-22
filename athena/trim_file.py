import pandas as pd
import pyarrow.parquet as pq
import s3fs

"""
This script takes every 100 lines of the AWS file and outputs into local drive.
"""
bucket="walmart-east-2-usama/"
path = "walmart_laptops_2018"
s3 = s3fs.S3FileSystem()
df = pq.ParquetDataset('s3://'+bucket+path, filesystem=s3).read_pandas().to_pandas()

sample = df.head(1000)

sample_output = sample.to_csv("walmart_sample_output.csv",index=False)

#If you print it to a file and have a quick look
x = sample.to_string(header=False,
                  index=False,
                  index_names=False).split('\n')

rows = [','.join(ele.split()) for ele in x]

for r in rows:
    print(r)
