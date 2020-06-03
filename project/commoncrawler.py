import requests
import argparse
import time
import json
from io import StringIO,BytesIO
import gzip
import csv
import codecs

from bs4 import BeautifulSoup


#here we are just parsing out our command line arguments and storing the result in our domain variable.
# parse the command line arguments
ap = argparse.ArgumentParser()
ap.add_argument("-d","--domain",required=True,help="The domain to target ie. https://www.amazon.com/")
args = vars(ap.parse_args())

domain = args['domain']

#this is a list of all of the Common Crawl indices that we can query for snapshots of the target domain.
# list of available indices
index_list = ["2014-52","2015-06","2015-11","2015-14","2015-18","2015-22","2015-27"]

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
        
        response = requests.get(cc_url)
        
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
def access_page(record):

    offset, length = int(record['offset']), int(record['length'])
    offset_end = offset + length - 1

    # We'll get the file via HTTPS so we don't need to worry about S3 credentials
    # Getting the file on S3 is equivalent however - you can request a Range
    prefix = 'https://aws-publicdatasets.s3.amazonaws.com/'
    
    # We can then use the Range header to ask for just this set of bytes
    resp = requests.get(prefix + record['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})
    print(resp.content) 
    # The page is stored compressed (gzip) to save space
    # We can extract it using the GZIP library
    #raw_data = StringIO(resp.content)
    #bytes
    raw_data = StringIO(resp.content)
    print(raw_data)
    f = gzip.GzipFile(fileobj=raw_data)
    
    # What we have now is just the WARC response, formatted:
    data = f.read()
    
    response = ""
    #now we split the data into three parts: the warc variable holds the metadata for the page in the WARC archive, the header variable has the HTTP 
    #headers retrieved when Common Crawl hit the target domain and the response variable contains the body of the HTML we want. If all is well we return 
    #the HTML data. 
    if len(data):
        try:
            warc, header, response = data.strip().split('\r\n\r\n', 2)
        except:
            pass
            
    return response

#
# Extract links from the HTML  
#
def extract_external_links(html_content,link_list):

    parser = BeautifulSoup(html_content)
        
    links = parser.find_all("a")
    
    if links:
        
        for link in links:
            href = link.attrs.get("href")
            
            if href is not None:
                
                if domain not in href:
                    if href not in link_list and href.startswith("http"):
                        print ("[*] Discovered external link: %s" % href)
                        link_list.append(href)

    return link_list




record_list = search_domain(domain)
link_list   = []

for record in record_list:
    
    html_content =  access_page(record)
    
    print ("[*] Retrieved %d bytes for %s" % (len(html_content),record['url']))
    
    link_list = extract_external_links(html_content,link_list)
    

print ("[*] Total external links discovered: %d" % len(link_list))

with codecs.open("%s-links.csv" % domain,"wb",encoding="utf-8") as output:

    fields = ["URL"]
    
    logger = csv.DictWriter(output,fieldnames=fields)
    logger.writeheader()
    
    for link in link_list:
        logger.writerow({"URL":link})
