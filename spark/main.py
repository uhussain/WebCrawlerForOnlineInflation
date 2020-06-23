##Adapted from https://github.com/tedhyu/Auto-Querect
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext,SparkSession, functions as F
from pyspark.sql.types import *
import re
import boto3
from warcio.archiveiterator import ArchiveIterator
from io import BytesIO
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
import argparse

def getComLineArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", "-in", type=str, default="", help="Input file name") 
    parser.add_argument("--output_path", "-o", type=str,default="", help="Output file name")
    
    return vars(parser.parse_args())

def html_to_text(page):
    """Converts html page to text
    Args:
        page:  html
    Returns:
        soup.get_text(" ", strip=True):  string
    """
    try:
        encoding = EncodingDetector.find_declared_encoding(page, is_html=True)
        soup = BeautifulSoup(page, "lxml", from_encoding=encoding)
        for script in soup(["script", "style"]):
            script.extract()
        return soup.get_text(" ", strip=True)
    except:
        return ""

def fetch_process_warc_records(rows):
    """Retrieves document from S3 Data Lake.  The argument is a row of warc_filename, warc_record_offset, warc_record_length.  
       The html to be retrieved are pulled from the file using the offset and length.  
    Args:
        rows: list[string, int, int]
    Returns:
        fetch_process_warc_records
    """
    s3client = boto3.client('s3')
    for row in rows:
        warc_path = row['warc_filename']
        offset = int(row['warc_record_offset'])
        length = int(row['warc_record_length'])
        rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
        response = s3client.get_object(Bucket='commoncrawl', Key=warc_path, Range=rangereq)
        record_stream = BytesIO(response["Body"].read())
        for record in ArchiveIterator(record_stream):
            page = record.content_stream().read()
            text = html_to_text(page)
            word_pattern = re.compile('\$+', re.UNICODE)
            iterator = word_pattern.finditer(text)
            #list of tuples (start,end) identifying all the places in the text where $ sign appears
            price_index=[]
            for match in iterator:
                price_index.append(match.span())
            sentences = []
            #grab substrings ranging from text[start-300:end+10] using the $index above and add them to sentences list
            for idx_tuple in price_index:
                if (idx_tuple[0]-300) < 0:
                    sentences.append(text[0:idx_tuple[1]+10])
                else:
                    sentences.append(text[idx_tuple[0]-300:idx_tuple[1]+10])
            for sen in sentences:
                title="None"
                price="None"
                if sen.find("$")!=-1:
                    price = sen[sen.find("$"):]
                    if (sen.find("Title")!=-1 and (sen.find("$")-100) > 0):
                        title=sen[sen.find("Title")+6:sen.find("$")-100]
                        yield title,price
                    else:
                        yield title,price
                else:
                    yield title,price

def main():

    args = getComLineArgs()
    conf = SparkConf().setAll([("spark.executor.memory", "10g"),
    ("spark.executor.instances", "8"),
    ("spark.executor.cores", "3"),
    ("spark.dynamicAllocation.enabled", "true"),])
    
    session = SparkSession.builder.master("yarn").config(conf=conf).getOrCreate() #Create Spark Session
    
    input_path = "s3://walmart-east-2-usama/%s" % (args['input_file'])
    sqldf = session.read.format("parquet").option("header", True).load(input_path+'/*')
    
    #Create rdd of the 1000 rows selected and manually repartition to avoid skew
    warc_recs = sqldf.select("warc_filename", "warc_record_offset", "warc_record_length").rdd.repartition(40)
    
    #print("NumOfPartitions: ",warc_recs.getNumPartitions())
    products= warc_recs.mapPartitions(fetch_process_warc_records)
    sqlContext = SQLContext(session)
     
    schemaProduct = sqlContext.createDataFrame(products,['Product','Price'])
    #Write to parquet format
    output_path = "s3://output-east-2-usama/%s/" % (args['output_path'])
    schemaProduct.write.parquet(output_path)

if __name__ == '__main__':
    #start_time = time.time()
    main()
    #print("Program took %s seconds" % (time.time()-start_time))
