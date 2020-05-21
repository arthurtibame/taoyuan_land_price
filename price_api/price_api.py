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

def crawler():
    #con = sqlite3.connect('D:\\python\\Chatbot\\GoBot\\shopAddress\\shop_details.db')
    con = sqlite3.connect('./data/data.db')
    config = configparser.ConfigParser()
    config.read(r'./price_api/price.ini')
    locations = config.sections() # list of locations        
    year = [i for i in range(100,110) if i!=104]

    try:

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
                    # json to dataframe
                    all_records = res['result']['records'][:]
                    df = pd.DataFrame(all_records)
                                       
    
                    #print(df2)
                    df = df.drop(columns=['_id','公告地價'])
                    df.insert(0,'年份', [year[k]]*len(df))
                   
                    #print(df)
                    #df.to_sql(str(locations[i]) ,con=con,index=False, if_exists='append')
                    df.to_csv("{}_{}.csv".format(locations[i],year[k]),index=None,mode='a', header=False)
                    #df2.to_csv("{}_{}.csv".format(locations[0],"公告地價"),index=None,mode='a', header=False)
                    offset+=10000
    except :
        print('something wrong in : {}, {}, {}'.format(locations[i], year[k], offset ))
    return 'finish'
        

def main():
    pass
    
            

if __name__ == "__main__":
    crawler()





#c = pd.DataFrame([b])
#print(c)

