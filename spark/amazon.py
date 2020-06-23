from bs4 import BeautifulSoup
import re
import boto3
from warcio.archiveiterator import ArchiveIterator
from io import BytesIO
def extract_product_data(html_content):

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
    #rating=""
    #if soup.select("#acrCustomerReviewText"):
    #        rating = (soup.select("#acrCustomerReviewText")[0].get_text().split()[0])
    return title,price

#Function for Amazon
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
            title,price = extract_product_data(html_response)
            yield title,price
