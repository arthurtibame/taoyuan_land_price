from tqdm import tqdm
import configparser
import requests
from requests.exceptions import ConnectionError
from requests.exceptions import ReadTimeout
import time
import pandas as pd
import sqlite3


def requests_get(*args1, **args2):
    i = 3
    while i >= 0:
        try:
            return requests.get(*args1, **args2)
        except (ConnectionError, ReadTimeout) as error:
            print(error)
            print('retry one more time after 60s', i, 'times left')
            time.sleep(60)
        i -= 1
    return pd.DataFrame()

def requests_post(*args1, **args2):
    i = 3
    while i >= 0:
        try:
            return requests.post(*args1, **args2)
        except (ConnectionError, ReadTimeout) as error:
            print(error)
            print('retry one more time after 60s', i, 'times left')
            time.sleep(60)
        i -= 1
    return pd.DataFrame()

def crawler(location):
    #con = sqlite3.connect('D:\\python\\Chatbot\\GoBot\\shopAddress\\shop_details.db')
    con = sqlite3.connect('./data/data.db')
    config = configparser.ConfigParser()
    config.read(r'./price_api/price.ini')
        
    year = [i for i in range(101,110) if i!=104]
    try:
        for i in tqdm(range(len(year))):
            url = config.get(str(location), str(year[i]))
            res = requests.get(url).json()
            # set the offset
            offset = 0
            # set the limit
            limit=20000
            # get total then request again to control the loop amount and loop amount = total/limit + 1
            loop_amount = res['result']['total']//limit+1        
            for j in range(loop_amount):

                url1 = url + '&&limit=' + str(limit) + '&&offset=' + str(offset)
                res = requests.get(url1).json()
                # json to dataframe
                all_records = res['result']['records'][:]
                df = pd.DataFrame(all_records)  
                df = df.drop(columns=['_id'])
                df.insert(0,'年份', [year[i]]*len(df))
                df.to_sql(str(location) ,con=con,index=False, if_exists='append')
                #df.to_csv("{}_{}-{}.csv".format(location,str(year[i]),j),index=None)
                offset+=10000
    except:
        print('something wrong')
    return 'finish'
        

def main():
    config = configparser.ConfigParser()
    config.read(r'./price_api/price.ini') 
    locations = config.sections() # list of locations
    
    for i in tqdm(range(len(locations))):        
        crawler(str(locations[i]))
            

if __name__ == "__main__":
    main()    





#c = pd.DataFrame([b])
#print(c)

