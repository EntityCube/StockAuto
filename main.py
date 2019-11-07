# NSE AUTO STOCK MANAGER
# Date Created: _/11/2019
# Author: EntityCuber
# Email: haktekguys@gmail.com


# importing all modules required
from nsetools import Nse
import sqlite3
from pprint import pprint
from multiprocessing import Process, current_process
import os
import time
import math


nse = Nse()  # initialising nse
conn = sqlite3.connect('mdata') #ql setup
c = conn.cursor() # sql object for sql scanning
iteration = 0
conn.execute("PRAGMA journal_mode=WAL")

choice = int(input("GRAB,SHOW (0,1)"))
if (choice == 1):
    c.execute('SELECT * FROM mdata')
    pprint(c.fetchall())
    exit()
elif (choice == 0):
    pass

# check for sql table exists , if not create one 
def table_spawn_check():
    table_exists = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mdata'").fetchone()


    if (not table_exists):
        print("table not found creating table")
        # creating a table with all expected datas from nse api
        c.execute('''
                  CREATE TABLE mdata(symbol text, sellPrice real, quantityTraded, totalSellQuantity real, totalBuyQuantity real, open real, dayHigh real, dayLow real, change real, closePrice real, totalTradedValue, totalTradedVolume, varMargin)''')

    else:
        c.execute('DELETE FROM mdata')


# get current list of all companies registered in nse
def get_stock_codes():
    global stock_codes
    global stock_codes_length
    stock_codes = nse.get_stock_codes()
    stock_codes.pop('SYMBOL') # removes first element as the first one is title
    print("Fetched stock codes")
    stock_codes = list(stock_codes.keys()) # converting dict element to list
    stock_codes_length = len(stock_codes) 


# fetch data. This function is multiprocessed by thread_fetch function
def fetch(stock):
    global stock_codes_length


    time.sleep(0.2)
    process_name = current_process().name
    fetch_progress = "%.2f" % ((int(process_name.split('-')[1])/stock_codes_length)*100)
    # print(f'fetching {stock} {process_name}/{stock_codes_length} {fetch_progress}')
    global response


    # Tries to collect data , if fails retry few times
    for i in range(0,5):   
        try:
            response = nse.get_quote(stock)
            # response = nse.get_quote(stock)
            print(f'fetched {response["symbol"]} {process_name}/{stock_codes_length} {fetch_progress}')
            # analyse fetched data
            analyse_stock(response)
        except (IndexError, ValueError):
            time.sleep(0.2)
            print(f"---------------------Error fetching: {stock} retrying:{i+1}/5 ")
            continue
        break


    



# multiple processing of fetch function
# need optimistation !IMPORTANT
def thread_fetch(stock):
    global processes
    processes = []
    process = Process(target=fetch, args=(stock,)) 
    processes.append(process)
    print(processes)
    process.start()
    time.sleep(0.5)

    # fetch(stock)

# collecting stocks using nse api
def collect_stocks():
    global processes
    global respo
    global stock_codes
    # for stock in stock_codes: # iterate through all stock codes collected before
    for stock in range(5)jj: 
        if (nse.is_valid_code(stock)): # testing whether stock code is valid or not
            thread_fetch(stock)
    
    # process join 
    for process in processes:
        process.join()

    print("Multiprocessing Complete")



def analyse_stock(response):
    # pass to sql section for further modification with sql

    # convert None to null for storing null value in sqlite integer columns
    for key, value in response.items():
        if (value == None):
            response[key] = 'null'


    sql_section(response)


                  #CREATE TABLE mdata(symbol text, sellPrice real, quantityTraded, sellQuantity real, buyQuantity real, open real, dayHigh real, dayLow real, change real, closePrice real, totalTradedValue, totalTradedVolume, varMargin)'''
def sql_section(value):
    command = f"""INSERT INTO mdata VALUES 
                 (

                '{value['symbol']}',
                {value['sellPrice1']},
                {value['quantityTraded']},
                {value['totalSellQuantity']},
                {value['totalBuyQuantity']},
                {value['open']},
                {value['dayHigh']},
                {value['dayLow']},
                {value['change']},
                {value['closePrice']},
                {value['totalTradedValue']},
                {value['totalTradedVolume']},
                {value['varMargin']}


                 )"""

    for i in range(0,100):
        try: 
            c.execute(command)
        except sqlite3.OperationalError as e:
            print(f"----------------------- Fixing: {e} try:{i+1}/100")
            continue
        break

    conn.commit()



if __name__ == '__main__':
    start_time = time.time()
    table_spawn_check()
    get_stock_codes()
    collect_stocks()
    end_time = time.time() - start_time
    print(f"Took {end_time/60} to complete")

