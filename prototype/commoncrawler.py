import requests
import argparse
import time
import json
from io import StringIO,BytesIO
import gzip
import csv
import codecs
from selectorlib import Extractor
from bs4 import BeautifulSoup
import re
from warcio.archiveiterator import ArchiveIterator

#Usage
#python project/commoncrawler.py -d https://insightfellows.com/
#here we are just parsing out our command line arguments and storing the result in our domain variable.
# parse the command line arguments
ap = argparse.ArgumentParser()
ap.add_argument("-d","--domain",required=True,help="The domain to target ie. https://insightfellows.com/")
args = vars(ap.parse_args())

domain = args['domain']

#this is a list of all of the Common Crawl indices that we can query for snapshots of the target domain.
# list of available indices
#index_list = ["2014-52","2015-06","2015-11","2015-14","2015-18","2015-22","2015-27"]
index_list = ["2019-04"]
              #,"2019-09","2019-13","2019-18","2019-22","2019-26","2019-30","2019-35","2019-39","2019-43","2019-47","2019-51","2020-05","2020-10","2020-16"]
#
# Searches the Common Crawl Index for a domain.
#

def search_domain(domain):
    record_list = []
    print ("[*] Trying target domain: %s" % domain)
    
    for index in index_list:
        print ("[*] Trying index %s" % index)
        cc_url  = "http://index.commoncrawl.org/CC-MAIN-%s-index?" % index
        cc_url += "url=%s&matchType=domain&output=json" % domain
        print("cc_url: ",cc_url)
        response = requests.get(cc_url)
        print("responseStatus: ",response.status_code) 
        if response.status_code == 200:
            records = response.content.splitlines()
            for record in records:
                record_list.append(json.loads(record))  
            print ("[*] Added %d results." % len(records))
    print ("[*] Found a total of %d hits." % len(record_list))
    return record_list        

#
# Downloads a page from Common Crawl - adapted graciously from @Smerity - thanks man!
# https://gist.github.com/Smerity/56bc6f21a8adec920ebf
#

def download_page(record):
    #if record['status']!='200':
    #    return
    offset, length = int(record['offset']), int(record['length'])
    offset_end = offset + length - 1
    
    # We'll get the file via HTTPS so we don't need to worry about S3 credentials
    # Getting the file on S3 is equivalent however - you can request a Range
    prefix = 'https://commoncrawl.s3.amazonaws.com/'
    #print(prefix + record['filename']) 
    print("Status: ",record['status'])
    # We can then use the Range header to ask for just this set of bytes
    resp = requests.get(prefix + record['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)},stream=True)
    #cc_url = "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-16/segments/1585370490497.6/warc/CC-MAIN-20200328074047-20200328104047-00000.warc.gz"
    #resp = requests.get(cc_url)
    # The page is stored compressed (gzip) to save space
    # We can extract it using the GZIP library
    raw_data= BytesIO(resp.raw.read())
    f = gzip.GzipFile(fileobj=raw_data)
    # What we have now is just the WARC response, formatted:
    data = f.read()
    response = ""
    print(type(data))
    #print(data)
    warc_input = data.decode("utf-8","ignore")
    #warc_input = str(data,'utf-8')
    #print(warc_input)
    if len(warc_input):
        #try:
        #print("Does it go here?")
        warc, header, response = warc_input.split('\r\n', 2)
            #warc, response = warc_input.strip().split('\r\n\r\n', 2)
        #except:
           # pass
    print("warc: ", warc)
    print("header: ", header)
    #print("response: ",response)
    #target_url = response.split("WARC-Target-URI: ")[1].split("\n")
    #try:
    #    status = re.search('HTTP/1.1(.+?)\n',response).group(1)
    #    if '200' in status:
    #        print("status: ",status)
    #        return response
    #except: 
    #    status=''
    #    return
    return response
    ########################WARC Iterator#########
    """
    response="" 
    for record in ArchiveIterator(resp.raw, arc2warc=True):
        if record.rec_type == 'warcinfo':
            print(record.raw_stream.read())

        elif record.rec_type == 'response':
            if record.http_headers.get_header('Content-Type') == 'text/html':
                print(record.rec_headers.get_header('WARC-Target-URI'))
                response = record.content_stream().read()
                #print(record.content_stream().read())
                #print('')
    return response
   """
#
# Extract links from the HTML  
#
def extract_product_links(html_content,link_list):

    parser = BeautifulSoup(html_content,"html.parser")
       
    links = parser.find_all("a")
    
    if links:
        
        for link in links:
            href = link.attrs.get("href")
            
            if href is not None:
                
                if domain in href and "/dp/" in href:
                    if href not in link_list and href.startswith("http"):
                        print ("[*] Discovered external link: %s" % href)
                        link_list.append(href)

    return link_list

def extract_product_data(html_content):

    # Create an Extractor by reading from the YAML file
    #e = Extractor.from_yaml_file('PokemonShop.yml')
    soup = BeautifulSoup(html_content,"html.parser")
    #print(soup.prettify())

    title=""
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
    rating=""
    if soup.select("#acrCustomerReviewText"):
            rating = (soup.select("#acrCustomerReviewText")[0].get_text().split()[0]) 
    return title,price,rating
    #print(data)
    #return title

def record_status_bad(record_dict):
    if record_dict['status']=='200':
        return True

def record_is_product(record_dict):
    if '/dp/' in record_dict['url']:
        return True

#Check for bad status
initial_record_list = search_domain(domain)
cleaned_record_list = list(filter(record_status_bad,initial_record_list))

#Check for product pages
record_list = list(filter(record_is_product,cleaned_record_list))

print("Total pages with products: ",len(record_list))

link_list   = []
with open('AmazonProducts.json','a') as outfile:
    #for url in urllist.read().splitlines():
        #data = scrape(url) 
        #print("data: ",data)
    #for i in range(1):
    for i in range(len(record_list)):
        record = record_list[i]
        html_content =  download_page(record)
        #print('html: ',html_content)
        if html_content is None:
            continue
        title,price,rating = extract_product_data(html_content)
        url = record['url']
        #htmlfile = open("html_content_%s"%i,"w")
        #htmlfile.writelines(url)
        #htmlfile.write("\n")
        #html_out = BeautifulSoup(html_content,"html.parser")
        #htmlfile.write(html_out.prettify())
        #htmlfile.close()

        ##Get links of external products on the page
        #link_list = extract_product_links(html_content,link_list)

        #Because I want to see all products with "dp" in their url
        if url:
            #title = product_info[0]
            #price = product_info[1]
            if not title:
                title = url.strip("https://www.amazon.com/").split("/dp")[0]
            jsonObject = {'title':title,'price': price,'url':url,'ratings':rating}
            print("title: ",title)
            print("price: ",price)
            print("ratings: ",rating)
            print("url: ",url)
            json.dump(jsonObject,outfile)
            outfile.write("\n")


#print ("[*] Total external links discovered: %d" % len(link_list))
##print("record_list:",record_list)
#with codecs.open("links.csv" ,"wb",encoding="utf-8") as output:
#
#    fields = ["URL"]
#    
#    logger = csv.DictWriter(output,fieldnames=fields)
#    logger.writeheader()
#    
#    for link in link_list:
#        logger.writerow({"URL":link})
