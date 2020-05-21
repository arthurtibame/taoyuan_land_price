import os
import glob
import pandas as pd
import csv
import configparser

config = configparser.ConfigParser()
config.read(r'./price_api/price.ini')
locations = config.sections() # list of locations     
for i in range(len(locations)):
    dir_ ="E:\\python\\桃園市各區土地公告現值及公告地價\\data\\"+str(locations[i]) +".csv"
    #col_names = ['年分','行政區', '段小段', '地號', '公告現值', '公告地價']
    df = pd.read_csv(dir_,error_bad_lines=False)
    #df = pd.read_csv(dir_,delimiter="\t", quoting=csv.QUOTE_NONE, encoding='utf-8')
    df = df.where(df.notnull(), 0) 
    df.to_csv("{}.csv".format(locations[i]),index=None, header=False)