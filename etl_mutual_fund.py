##--------------------------------------------------------------------------------------
##--------------------------------------------------
##---------------------------
## Script_Name - ETL_MUTUAL_FUNDS
## Author Name - RAKESH PURTY
## Version - 1.0
## Date - 09-01-2022
##---------------------------
##--------------------------------------------------
## Importing required libraries
##---------------------------
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import mysql.connector
import datetime
from datetime import date
from datetime import timedelta
import time
##---------------------------
##--------------------------------------------------
## function to connect to workbench and create database and tables
## connecting to workbench with db name (if the db is not created it will throw an error with unknown database and hence need to create database first and than connect to create table)
def connect_to_workbench(database,mf_nd_number,conn_details):
    try:
        mydb = mysql.connector.connect(host=conn_details["host"],user=conn_details["user"],passwd=conn_details["password"],database=conn_details["database"])
        cursor = mydb.cursor()
        return {"cursor":cursor,"mydb":mydb}
    except Exception as e:
         message = str(e)
    try:
        if 'Unknown database' in message:
            mydb = mysql.connector.connect(host=conn_details["host"],user=conn_details["user"],passwd=conn_details["password"])
            cursor = mydb.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(database))
            mydb = mysql.connector.connect(host=conn_details["host"],user=conn_details["user"],passwd=conn_details["password"],database=conn_details["database"])
            cursor = mydb.cursor()
            for i,j in mf_nd_number.items():
                cursor.execute("""CREATE TABLE IF NOT EXISTS {0} (Scheme_Code int,ISIN_Div_Payout_ISIN_Growth varchar(255),\
                                ISIN_Div_Reinvestment varchar(255),Scheme_Name varchar(255),Net_Asset_Value double,\
                                modified_date date,scheme_type varchar(30));""".format(i))
            return {"cursor":cursor,"mydb":mydb}
        else:
            return {'message':message}
    except Exception as e:
        print(e)
        
## function to fetch the data from url
def data_from_url(url,scheme_type):
    try:
        ## reading the text file from url in csv to store it in dstaframe
        df = pd.read_csv(url, header=None, sep=';')
        ## making the first row as header
        new_header = df.iloc[0] 
        df = df[1:] 
        df.columns = new_header 
        df.columns = df.columns.str.replace(' ','_').str.replace('/_','_')
        ## creating one column scheme_type to add scheme type value
        if scheme_type == "Open":
            df['scheme_type'] = 'Open Ended Schemes'
        elif scheme_type == "Close":
            df['scheme_type'] = 'Close Ended Schemes'
        else:
            df['scheme_type'] = 'Interval Fund'
        df.dropna(inplace=True)
        return df
    except Exception as e:
        print(e)

## function to load the data to workbench
## Filtering on Scheme_Name column to filter only particular mutual funds data and storing it into mysql table
## Franklin Templeton,Kotak Mahindra,Trust,Canara Robeco,Nippon India are the mutual funds whose value in scheme name start with either uppercase or with different name so is handled accordingly
def load_data_to_mysql(cursor,mydb,df,key,value,table_list_number):
    for i,j in table_list_number.items():
        if j == value:
            funds = i
    if funds == "Franklin Templeton":
        df_new = df[df['Scheme_Name'].str.contains("Franklin")]
    elif funds == "Kotak Mahindra":
        df_new = df[df['Scheme_Name'].str.contains("Kotak")]
    elif funds == "Trust":
        df_new = df[df['Scheme_Name'].str.contains("TRUSTMF")]
    elif funds == "Canara Robeco":
        df_new = df[df['Scheme_Name'].str.contains("CANARA ROBECO")]
    elif funds == "Nippon India":
        df_new = df[df['Scheme_Name'].str.contains("NIPPON INDIA")]
    else:   
        df_new = df[df['Scheme_Name'].str.contains(funds)]
    df_new.rename(columns = {'Date':'modified_date'}, inplace = True)
    df_new['modified_date'] = pd.to_datetime(df_new['modified_date']).dt.date
    try:
        for i,row in df_new.iterrows():
            sql = """INSERT INTO `{0}` (`Scheme_Code`, `ISIN_Div_Payout_ISIN_Growth`, `ISIN_Div_Reinvestment`, `Scheme_Name`, `Net_Asset_Value`, `modified_date`, `scheme_type`) VALUES (%s, %s, %s, %s, %s, %s,%s)""".format(key)
            cursor.execute(sql, tuple(row))
            mydb.commit()
    except Exception as e:
        print(e)

## function to check the type of load onetime or incremental and load the data accordingly to workbench
## Querying on the table first to check if empty or not, if empty than it's full load and to avoid hitting url again for full load tookfirst mutual fund from the dict and saved in dataframe
## For incremental for particualr mutual fund and there correspond mf number and yesterday date is passed on the url to fetch the data but before that the date is passed in where clause to make sure no double incremental load is done
def fetch_data_nd_load (mf_nd_number,cursor,mydb,scheme_type,start_nd_end_date,table_list_number,query_date):
    try:
        if scheme_type == "Open":
            for key,value in mf_nd_number.items():
                sql = "SELECT * FROM {}".format(key)
                cursor = mydb.cursor(buffered=True)
                cursor.execute(sql)
                myresult = cursor.fetchone()
                if not myresult == 0:
                    url_open_onetime = "https://www.amfiindia.com/spages/NAVOpen.txt"
                    if key == "abn__amro_mutual_fund":
                        df = data_from_url(url_open_onetime,'Open')
                    load_data_to_mysql(cursor,mydb,df,key,value,table_list_number)  
                else:
                    sql = "SELECT * FROM {0} where modified_date = {1} and scheme_type = 'Open Ended Schemes' ".format(key,query_date)
                    cursor = mydb.cursor(buffered=True)
                    cursor.execute(sql)
                    myresult = cursor.fetchone()
                    if not myresult == 0:
                        url_open_incremental = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf={0}&tp=1&frmdt={1}&todt={1}".format(value,start_nd_end_date)
                        df = data_from_url(url_open_incremental,'Open')
                        load_data_to_mysql(cursor,mydb,df,key,value,table_list_number)
                    else:
                        return print({"message":"Data has already loaded for {}".format(key)})

        elif scheme_type == "Close":
            for key,value in mf_nd_number.items():
                sql = "SELECT * FROM {}".format(key)
                cursor = mydb.cursor(buffered=True)
                cursor.execute(sql)
                myresult = cursor.fetchone()
                if not myresult == 0:
                    url_close_onetime = "https://www.amfiindia.com/spages/NAVClose.txt"
                    if key == "abn__amro_mutual_fund":
                        df = data_from_url(url_close_onetime,'Close')
                    load_data_to_mysql(cursor,mydb,df,key,value,table_list_number)  
                else:
                    sql = "SELECT * FROM {0} where modified_date = {1} scheme_type = 'Close Ended Schemes' ".format(key,query_date)
                    cursor = mydb.cursor(buffered=True)
                    cursor.execute(sql)
                    myresult = cursor.fetchone()
                    if not myresult == 0:
                        url_close_incremental = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf={0}&tp=2&frmdt={1}&todt={1}".format(value,start_nd_end_date)
                        df = data_from_url(url_close_incremental,'Close')
                        load_data_to_mysql(cursor,mydb,df,key,value,table_list_number)
                    else:
                        return print({"message":"Data has already loaded for {}".format(key)})

        else:
            for key,value in mf_nd_number.items():
                sql = "SELECT * FROM {}".format(key)
                cursor = mydb.cursor(buffered=True)
                cursor.execute(sql)
                myresult = cursor.fetchone()
                if not myresult == 0:
                    url_interval_fund_onetime = "https://www.amfiindia.com/spages/NAVInterval.txt"
                    if key == "abn__amro_mutual_fund":
                        df = data_from_url(url_interval_fund_onetime,'Interval')
                    load_data_to_mysql(cursor,mydb,df,key,value,table_list_number)
                else:
                    sql = "SELECT * FROM {0} where modified_date = {1} scheme_type = 'Interval Fund' ".format(key,query_date)
                    cursor = mydb.cursor(buffered=True)
                    cursor.execute(sql) 
                    myresult = cursor.fetchone()
                    if not myresult == 0:
                        url_interval_incremental = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf={0}&tp=3&frmdt={1}&todt={1}".format(value,start_nd_end_date)
                        df = data_from_url(url_interval_incremental,'Interval')
                        load_data_to_mysql(cursor,mydb,df,key,value,table_list_number)
                    else:
                        print({"message":"Data has already loaded for {}".format(key)})
    except Exception as e:
        print({'error':e,'table':key})
##--------------------------------------------------
## creating a dictionary of mutual fund and the mf number to fetch the incremental data from the url
##---------------------------
mf_number_details = {"ABN  AMRO Mutual Fund":39,"Aditya Birla Sun Life Mutual Fund":3,"AEGON Mutual Fund":50,"Alliance Capital Mutual Fund":1,
    "Axis Mutual Fund":53,"Baroda Mutual Fund":4,"Benchmark Mutual Fund":36,"BNP Paribas Mutual Fund":59,"BOI AXA Mutual Fund":46,
    "Canara Robeco Mutual Fund":32,"Daiwa Mutual Fund":60,"DBS Chola Mutual Fund":31,"Deutsche Mutual Fund":38,"DSP Mutual Fund":6,
    "Edelweiss Mutual Fund":47,"Fidelity Mutual Fund":40,"Fortis Mutual Fund":51,"Franklin Templeton Mutual Fund":27,
    "GIC Mutual Fund":8,"Goldman Sachs Mutual Fund":49,"HDFC Mutual Fund":9,"HSBC Mutual Fund":37,"ICICI Prudential Mutual Fund":20,
    "IDBI Mutual Fund":57,"IDFC Mutual Fund":48,"IIFCL Mutual Fund (IDF)":68,"IIFL Mutual Fund":62,"IL&F S Mutual Fund":11,
    "IL&FS Mutual Fund (IDF)":65,"Indiabulls Mutual Fund":63,"ING Mutual Fund":14,"Invesco Mutual Fund":42,"ITI Mutual Fund":70,
    "JM Financial Mutual Fund":16,"JPMorgan Mutual Fund":43,"Kotak Mahindra Mutual Fund":17,"L&T Mutual Fund":56,"LIC Mutual Fund":18,
    "Mahindra Manulife Mutual Fund":69,"Mirae Asset Mutual Fund":45,"Morgan Stanley Mutual Fund":19,"Motilal Oswal Mutual Fund":55,
    "Navi Mutual Fund":54,"Nippon India Mutual Fund":21,"NJ Mutual Fund":73,"PGIM India Mutual Fund":58,"PineBridge Mutual Fund":44,
    "PNB Mutual Fund":34,"PPFAS Mutual Fund":64,"Principal Mutual Fund":10,"quant Mutual Fund":13,"Quantum Mutual Fund":41,
    "Samco Mutual Fund":74,"SBI Mutual Fund":22,"Shinsei Mutual Fund":52,"Shriram Mutual Fund":67,"Standard Chartered Mutual Fund":2,
    "SUN F&C Mutual Fund":24,"Sundaram Mutual Fund":33,"Tata Mutual Fund":25,"Taurus Mutual Fund":26,"Trust Mutual Fund":72,
    "Union Mutual Fund":61,"UTI Mutual Fund":28,"YES Mutual Fund":71,"Zurich India Mutual Fund":29}
##---------------------------
##--------------------------------------------------
## creating a list of mutual funds and refining the name to name the tables
funds_list = [mf_name for mf_name,number in mf_number_details.items()]
for funds in range(len(funds_list)):
    funds_list[funds] = ((funds_list[funds].replace(' ', '_')).lower().replace('&', '_nd_').replace('(idf)', 'idf'))
    
## creating a list of mf_number
mf_number = [number for mf_name,number in mf_number_details.items()]
## creating a list of mutual_funds
table_list = [mf_name for mf_name,number in mf_number_details.items()]
convertor = lambda x:x.replace(' Mutual Fund','')
table_list = list(map(convertor,table_list))

## creating a dict of funds and there number and table name and mf_number
mf_nd_number = dict(zip(funds_list,mf_number))
table_list_number = dict(zip(table_list,mf_number))
##--------------------------------------------------
##---------------------------
## connection details
conn_details = {"host":"localhost","user":"root","password":"12345","database":'mutual_funds'}
##---------------------------
##--------------------------------------------------

## Calculating yesterday's date and date used for querying in the table for incremental load
yesterday = (date.today() - timedelta(days = 1)).strftime('%d-%m-%Y')
query_date = (date.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
month_num = yesterday.split('-')[1]
datetime_object = datetime.datetime.strptime(month_num, "%m")
month_name = datetime_object.strftime("%b")
yesterday_date = yesterday.split('-')
yesterday_date[1] = month_name
start_nd_end_date = '-'.join(yesterday_date)
##--------------------------------------------------

## connecting to workbench and creating db and tables
conn = connect_to_workbench(conn_details["database"],mf_nd_number,conn_details)
cursor = conn['cursor']
mydb = conn['mydb']
##--------------------------------------------------

## calling the function to load open scheme type data to tables
fetch_data_nd_load (mf_nd_number,cursor,mydb,'Open',start_nd_end_date,table_list_number,query_date)
##--------------------------------------------------

## calling the function to load close scheme type data to tables
fetch_data_nd_load (mf_nd_number,cursor,mydb,'Close',start_nd_end_date,table_list_number,query_date)
##--------------------------------------------------

## calling the function to load interval scheme type data to tables
fetch_data_nd_load (mf_nd_number,cursor,mydb,'Interval',start_nd_end_date,table_list_number,query_date)
##--------------------------------------------------
##--------------------------------------------------------------------------------------