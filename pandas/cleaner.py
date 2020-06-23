import pandas as pd
import numpy as np
import re
import boto3
import s3fs
from collections import defaultdict

def get_file_names(bucket_name,prefix):
    """
    Return the latest file name in an S3 bucket folder.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (folder  name).
    """
    s3_client = boto3.client('s3')
    objs = s3_client.list_objects_v2(Bucket=bucket_name)['Contents']
    shortlisted_files = []            
    for obj in objs:
        key = obj['Key']
        # if key starts with folder name retrieve that key
        if key.startswith(prefix) and "parquet" in key:              
            shortlisted_files.append(key)
    return shortlisted_files

#Return laptop_avg_prices_per_year
def laptop_categories_avg_price(price_level):
    avg_price_per_year = []
    years = ["2017","2018","2019","2020"]
    for yr in years:
        #print(yr)
        highlevel = price_level['year'] == yr
        highlevel_mean= price_level.loc[highlevel, 'Price'].mean()
        avg_price_per_year.append(highlevel_mean)
    
    return avg_price_per_year

def main():
    filenames=defaultdict(list)
    year = ["2016","2017","2018","2019","2020"]

    #Get file names
    for yr in year:
        filenames[yr] = get_file_names(bucket_name='output-east-2-usama',prefix = 'walmart_parquet_'+yr)
        #First file is not parquet file
        filenames[yr].pop(0)
        #print(filenames)
    
    #Creating pandas dataframes
    dataframes = []
    for yr in year:
        data = [pd.read_parquet("s3://output-east-2-usama/"+f) for f in filenames[yr]]
        df = pd.concat(data,ignore_index=True)
        #inserting a yr column
        df.insert(2,'year',yr)
        dataframes.append(df)
    
    #Cleaning the pandas dataframes
    for df in dataframes:
        #Dropping rows without Price information
        no_price = df[df.Price.astype(bool)]
        final = no_price[no_price.Product.astype(bool)]
        #Dropping duplicates
        dedup = final.drop_duplicates()
        Final = dedup.drop_duplicates(subset="Product")
        #Cleaning any spaces or unnecessary columns in the price field
        Final = Final.assign(Price=lambda x: x['Price'].str.replace(" ",""))
        Final = Final.assign(Price=lambda x: x['Price'].str.replace(",",""))
        #Getting price formatted
        Final = Final.assign(Price=lambda x: x['Price'].str.extract('^(\$?\d{1,20}\s*\.\s*\d{1,20})',expand=False))
        Final = Final.dropna()
        CleanDF.append(Final)
    
    #More cleaning
    Walmart_2016_2020 = pd.concat(CleanDF)
    
    prod = Walmart_2016_2020.assign(Product=lambda x: x['Product'].str.lstrip())
    prod =prod.assign(Price=lambda x: x['Price'].str.replace("$",""))
    prod['Price'] = pd.to_numeric(prod['Price'])
    
    #Expensive laptops
    high_exp = prod['Price'] > 1100
    exp_level = prod[high_exp]
    
    #Laptops ($1100-)
    high_level_laptops = laptop_categories_avg_price(exp_level)
    
    #Mid-level laptops
    low_mid = prod['Price'] > 800
    high_mid = prod['Price'] < 1100
    mid_level = prod[low_mid & high_mid]
    
    #Laptops ($800-$1100)
    mid_level_laptops = laptop_categories_avg_price(mid_level)
    
    #Only keep laptops more than $500 and less than 800 - "Affordable laptops"
    low = prod['Price'] > 500
    high = prod['Price'] < 800
    affordable = prod[low & high]
    
    #Laptops ($500-$800)
    affordable_laptops = laptop_categories_avg_price(affordable)
    
    #Rounding off to make it pretty 
    affordable_laptops=[round(num, 1) for num in affordable_laptops]
    mid_level_laptops=[round(num, 1) for num in mid_level_laptops]
    high_level_laptops=[round(num, 1) for num in high_level_laptops]
    
    laptops = {'Year':years,'Low-Level':affordable_laptops,'Mid-Level':mid_level_laptops,'High-Level':high_level_laptops}
    
    laptop_prices = pd.DataFrame(laptops, columns = ['Year', 'Low-Level','Mid-Level','High-Level'])
    laptop_prices.to_csv("dashapp/datasets/laptop_trends_2017-2020.csv",index=True)

if __name__ == '__main__':
    #start_time = time.time()
    main()
    #print("Program took %s seconds" % (time.time()-start_time))
