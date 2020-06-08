# Inspiration from https://www.bellingcat.com/resources/2015/08/13/using-python-to-mine-common-crawl/

## PRODUCT FINDER ##
# Scans common crawl infomation for products and
# saves them to Amazon DynomoDB database.

#Install
#   boto3: https://github.com/boto/boto3. Make sure to add AWS Credentials on our machine, see docs.
#   

#Test Make sure code is working with local example - Run
#   python productfinder_helper.py
#Run
#   python main --domain amazon.com

# Version 1.2

import requests
import argparse
import time
import json
from io import StringIO
import gzip
from bs4 import BeautifulSoup

#Own
from product import Product
from saveproducts import SaveProducts
from productfinder import ProductFinder


# parse the command line arguments
ap = argparse.ArgumentParser()
ap.add_argument("-d","--domain", required=True, help="The domain to target ie. youtube.com")
args = vars(ap.parse_args())

domain = args['domain']

# list of available indices, "2014-52"
# index_list = ["2017-39", "2017-34", "2017-30", "2017-26", "2017-22", "2017-17"]
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

def record_status_bad(record_dict):
    if record_dict['status']=='200':
        return True

def record_is_product(record_dict):
    if '/dp/' in record_dict['url']:
        return True
### -----------------------
###     Main Function
### -----------------------
def main():
    print("Starting CommonCrawl Search")
    #Finds all relevant domins
    initial_record_list = search_domain(domain)

    #Check for bad status
    initial_record_list = search_domain(domain)
    cleaned_record_list = list(filter(record_status_bad,initial_record_list))
    
    #Check for product pages
    record_list = list(filter(record_is_product,cleaned_record_list))

    print("Total pages with products: ",len(record_list))
    
    #Creating save object - Products are saved to Amazon DynamoDB
    #savethread = SaveProducts().start()
    #Downloads page from CommconCrawl and Inspects, then Extracts infomation
    product_finder_1 = ProductFinder(record_list)
    
    products = product_finder_1.update() 
    print("products_listLength: ",len(products)) 
    saveProducts=SaveProducts(products)
    saveProducts.update()

if __name__ == '__main__':
    main()
