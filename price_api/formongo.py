import requests
import pymongo
import configparser
import pandas as pd
from tqdm import tqdm

def crawler():
    config = configparser.ConfigParser()
    config.read(r'./price_api/price.ini')
    locations = config.sections() # list of locations 
    year = [i for i in range(100,110) if i!=104] # list of years   
    
    # get total then request again to control the loop amount and loop amount = total/limit + 1
    
    for i in tqdm(range(len(locations))):
        for k in tqdm(range(len(year))):
            url = config.get(str(locations[i]), str(year[k]))
            res = requests.get(url).json()
            # set the offset
            offset = 0
            # set the limit
            limit=10000
            # get total then request again to control the loop amount and loop amount = total/limit + 1
            loop_amount = res['result']['total']//limit+1        
            for j in range(loop_amount):

                url1 = url + '&&limit=' + str(limit) + '&&offset=' + str(offset)
                res = requests.get(url1).json()                
                all_records = res['result']['records'][:]
                # json add year
                for record_len in range(len(all_records)):
                    all_records[record_len]['year'] = str(year[k])
                
                #call json2mongo
                json2mongo(str(locations[i]),all_records )
                offset+=10000
def json2mongo(colname, jsonData):
    # set connection to GCP Mongo
    myclient = pymongo.MongoClient("mongodb://35.221.171.163:27017/")
    mydb = myclient["lin"]
    # create table if not exist
    collist = mydb.list_collection_names() #list all col names to check whether exist of not
    if str(colname) not in collist:
        # create table and insert data
        collection = mydb[str(colname)]        
        collection.insert_one(jsonData)
    
    
    #else insert data directly        
    else:
        collection = mydb[str(colname)]        
        collection.insert_one(jsonData)
        
        
    myclient.close()


if __name__ == "__main__":
    crawler()    