##Adapted from https://github.com/tedhyu/Auto-Querect
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext,SparkSession, functions as F
from pyspark.sql.types import *
import re
import boto3
#import nltk
from warcio.archiveiterator import ArchiveIterator
from io import BytesIO
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
#from nltk.stem.wordnet import WordNetLemmatizer
#nltk.download('punkt')
#Lem = WordNetLemmatizer()
#nltk.download('wordnet')
#nltk.download('words')
#from nltk import tokenize

def getComLineArgs():
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
def extract_product_data(html_content):

    # Create an Extractor by reading from the YAML file
    #e = Extractor.from_yaml_file('PokemonShop.yml')
    soup = BeautifulSoup(html_content,"html.parser")
    #print(soup.prettify())

    title=""
    #Amazon product titles grabbed using html classes
    if soup.select("#productTitle"):
        title = soup.select("#productTitle")[0].get_text().strip()
    elif soup.select("#btAsinTitle"):
        title = soup.select("#btAsinTitle")[0].get_text().strip()
    #product_info.append(title)
    price=""
    if soup.select("#priceblock_saleprice"):
        price = soup.select("#priceblock_saleprice")[0].get_text()
    elif soup.select("#priceblock_ourprice"):
        price = soup.select("#priceblock_ourprice")[0].get_text()
    else:
        #text = html_to_text(html_content)
        text = soup.get_text()
        #Look for $ prices in text
        #print(text)
        word_pattern = re.compile('\$+', re.UNICODE)
        iterator = word_pattern.finditer(text)
        #list of tuples (start,end) identifying all the places in the text where $ sign appears
        price_index=[]
        for match in iterator:
            price_index.append(match.span())
        sentences = []
        #grab substrings ranging from text[start-300:end+10] using the $index above and add them to sentences list
        #print("price_index: ",price_index)
        for idx_tuple in price_index:
            if (idx_tuple[0]-300) < 0:
                sentences.append(text[0:idx_tuple[1]+10])
            else:
                sentences.append(text[idx_tuple[0]-300:idx_tuple[1]+10])
        #print(sentences)
        for sen in sentences:
            if sen.find("$")!=-1:
                price = sen[sen.find("$"):]
                #Walmart product titles grabbed from text
                if title=="":
                    if (sen.find("Title")!=-1 and (sen.find("$")-100) > 0):
                        title=sen[sen.find("Title")+6:sen.find("$")-100]
    rating=""
    if soup.select("#acrCustomerReviewText"):
            rating = (soup.select("#acrCustomerReviewText")[0].get_text().split()[0])
    return title,price,rating

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
        #row = rows[i]
        warc_path = row['warc_filename']
        offset = int(row['warc_record_offset'])
        length = int(row['warc_record_length'])
        rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
        response = s3client.get_object(Bucket='commoncrawl', Key=warc_path, Range=rangereq)
        #print(response)
        record_stream = BytesIO(response["Body"].read())
        #print(record_stream)
        for record in ArchiveIterator(record_stream):
            page = record.content_stream().read()
            #print(type(page))
            html_response=""
            warc_input = page.decode("utf-8","ignore")
            #print(warc_input)
            try:
                warc, header, html_response = warc_input.split('\r\n', 2)
            except:
                pass
            #text = html_to_text(page)
            #text = udf_text(page)
            #print(text)
            #print("WARC: ",warc)
            #print("HEADER: ",header)
            title,price,rating = extract_product_data(html_response)
            yield title,price,rating

#def fetch_process_warc_records(rows):
#    """Retrieves document from S3 Data Lake.  The argument is a row of warc_filename, warc_record_offset, warc_record_length.  
#       The html to be retrieved are pulled from the file using the offset and length.  
#    Args:
#        rows: list[string, int, int]
#    Returns:
#        fetch_process_warc_records
#    """
#    s3client = boto3.client('s3')
#    for row in rows:
#        warc_path = row['warc_filename']
#        offset = int(row['warc_record_offset'])
#        length = int(row['warc_record_length'])
#        rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
#        response = s3client.get_object(Bucket='commoncrawl', Key=warc_path, Range=rangereq)
#        record_stream = BytesIO(response["Body"].read())
#        for record in ArchiveIterator(record_stream):
#            page = record.content_stream().read()
#            text = html_to_text(page)
#            #words = map(lambda w: w, word_pattern.findall(text))
#            #result = tokenize.sent_tokenize(text)
#            #sentences = map(lambda s: s,tokenize.sent_tokenize(text))
#            #final = list(filter(lambda x: ('$' in x),result))
#            iterator = word_pattern.finditer(text)
#            #list of tuples (start,end) identifying all the places in the text where $ sign appears
#            price_index=[]
#            for match in iterator:
#                price_index.append(match.span())
#            sentences = []
#            #grab substrings ranging from text[start-300:end+10] using the $index above and add them to sentences list
#            for idx_tuple in price_index:
#                if (idx_tuple[0]-300) < 0:
#                    sentences.append(text[0:idx_tuple[1]+10])
#                else:
#                    sentences.append(text[idx_tuple[0]-300:idx_tuple[1]+10])
#            for sen in sentences:
#                title="None"
#                price="None"
#                if sen.find("$")!=-1:
#                    price = sen[sen.find("$"):]
#                    if (sen.find("Title")!=-1 and (sen.find("$")-100) > 0):
#                        title=sen[sen.find("Title")+6:sen.find("$")-100]
#                        yield title,price
#                    else:
#                        yield title,price
#                else:
#                    yield title,price
#            #for word in words:
#            #    yield word,1   #create key as word and index as 1
args = getComLineArgs()
conf = SparkConf().setAll([("spark.executor.memory", "10g"),
("spark.executor.instances", "8"),
("spark.executor.cores", "3"),
("spark.dynamicAllocation.enabled", "true"),])

session = SparkSession.builder.master("yarn").config(conf=conf).getOrCreate() #Create Spark Session

#Read csv from athena output.  Take rows
#sqldf = session.read.format("csv").option("header", True).option("inferSchema", True).load("s3://athena-east-2-usama/Unsaved/2020/06/09/112be0b3-7589-4b70-a240-10d781c28b60.csv")

#Read parquet from athena query output
#sqldf = session.read.format("parquet").option("header", True).load('s3://athena-east-2-usama/Amazon_lap_2020_10/*')
input_path = "s3://walmart-east-2-usama/%s" % (args['input_file'])
sqldf = session.read.format("parquet").option("header", True).load(input_path+'/*')
#sqldf = sqldf.repartition(12)
#Read locally
#sqldf = session.read.format("csv").option("header", True).option("inferSchema", True).load("walmart.csv")

#Create rdd of the 1000 rows selected
warc_recs = sqldf.select("warc_filename", "warc_record_offset", "warc_record_length").rdd

print("NumOfPartitions: ",warc_recs.getNumPartitions())
#Look for $ prices in text
#word_pattern = re.compile('\$+', re.UNICODE)

#convert to unicode
#word_pattern = re.compile('\w+', re.UNICODE)

#mapPartition gets a list of words and 1's.  Filter removes all words that don't start with capital.  reduceByKey combines all a's and gets word count.  sortBy sorts by largest count to smallest. 
#word_counts = warc_recs.mapPartitions(fetch_process_warc_records).filter(lambda x: ('$' in x ,x[0]))
#word_counts = warc_recs.mapPartitions(fetch_process_warc_records).filter((lambda a: re.search(r'^[A-Z][a-z]', a[0]))).reduceByKey(lambda a, b: a + b).sortBy(lambda a: a[1], ascending=False)
#print("Rdd: ",word_counts)

#products= warc_recs.mapPartitions(fetch_process_warc_records).filter((lambda a: re.search(re.compile('None*'),a[0])))

#filter on all products that have title None
## Filter out duplicates with distinct()
products= warc_recs.mapPartitions(fetch_process_warc_records)
#.filter((lambda a: "None" not in a[0]))
#.distinct()
sqlContext = SQLContext(session)

#word_list= list(word_counts.take(1000))
#new_list=[]
#for i in word_list:
#    if i[0].lower() not in nltk.corpus.wordnet.words():  #filters by wordnet library
#        lower_case = Lem.lemmatize(i[0].lower())         #converts any plurals to singular
#        if lower_case not in nltk.corpus.words.words():  #filter by words library
#            new_list.append(i)                           #adds to list.

# Infer the schema, and register the DataFrame as a table.

#schemaWordCounts = sqlContext.createDataFrame(new_list,['Word','Frequency'])
#Product_Price = list(products)
#print(Product_Price)                                  

schemaProduct = sqlContext.createDataFrame(products,['Product','Price','Rating'])
#print("schema: ",schemaWordCounts) 
#=> schema:  DataFrame[_1: string, _2: bigint]

#Write to parquet format
#schemaProduct.write.parquet("s3://athena-east-2-usama/Amazon_Products_2018-47/")
output_path = "s3://output-east-2-usama/%s/" % (args['output_path'])
schemaProduct.write.csv(output_path)
#Write to csv format
#schemaProduct.write.csv("s3://output-east-2-usama/Walmart_laptops_2019_09/")
#print(word_list)
