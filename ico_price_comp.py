# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import pymysql
from urllib import parse
import re
import bs4
import time
import sys
from importlib import reload

def get_token():
    cur.execute("select symbol from coin_market_data where isnull(state) or state='' limit 1")
    ico_token = cur.fetchall()[0][0]
    return ico_token

def price_comp(ico_token,medium_token,rate,medium_flag):
    #flag == -1 means cannot find medium so we can not compare the price
    if medium_flag==-1:
        cur.execute("UPDATE coin_market_data SET state='N' WHERE symbol='" + ico_token + "'")
        conn.commit()
        return -1

    cur.execute("select date,open from coin_market_data where symbol='" + ico_token + "'")
    ico_info = cur.fetchall()
    if not ico_info:
        return -1

    date = ico_info[0][0]
    cur_price = float(ico_info[0][1])

    #GET START AND END TIME
    cur.execute("select start,end from icobench where token='"+ico_token+"'")
    times = cur.fetchall()
    if len(times)>0:
        if len(times[0])==2:
            start_time = times[0][0]
            end_time = times[0][1]
        else:
            start_time = str(None)
            end_time = str(None)
    else:
        start_time = str(None)
        end_time = str(None)

    #if medium is currency, just compare directly
    if medium_flag == 2:
        result = 'UP' if cur_price>rate else 'DOWN'
        cur.execute("UPDATE coin_market_data SET state='Y',ico_price='"+str(rate)+"',result='"+result+"',start='"+start_time+"',end='"+end_time+"'  WHERE symbol='" + ico_token + "'")
        conn.commit()
        return result
    else:
        cur.execute("select coin_id from coin_market_data where symbol='" + medium_token + "'")
        medium_info = cur.fetchall()
        if len(medium_info)==0:
            cur.execute("UPDATE coin_market_data SET state='N' WHERE symbol='" + ico_token + "'")
            return 'ERROR'
        medium_id = medium_info[0][0]

        cur.execute("select open from coin_historical_data where coin_id='" + medium_id + "' and date='" + date + "'")
        coin_history = cur.fetchall()
        if coin_history:
            medium_price = float(coin_history[0][0])
        else:
            cur.execute("UPDATE coin_market_data SET state='N' WHERE symbol='" + ico_token + "'")
            conn.commit()
            print('history price not found')
            return -1
        his_price = medium_price*rate
        result = 'UP' if cur_price > his_price else 'DOWN'
        print("UPDATE coin_market_data SET state='Y',ico_price='"+str(his_price)+"',result='" + result + "',start='"+start_time+"',end='"+end_time+"' WHERE symbol='" + ico_token + "'")
        cur.execute("UPDATE coin_market_data SET state='Y',ico_price='"+str(his_price)+"',result='" + result + "',start='"+start_time+"',end='"+end_time+"'  WHERE symbol='" + ico_token + "'")
        conn.commit()
        return result
def reform_price(token):
    cur.execute("select price_in_ico from icobench_financial where token='"+token+"'")
    price = cur.fetchall()
    price_flag=0
    if not price:
        return token, 0, -1
    for i in price:
        if i[0] is not None:
            print(i[0])
            price=i[0]
            price_flag=1
        else:
            continue
    if price is None:
        return token, 0, -1
    if price_flag == 0:
        return token, 0, -1

    v = price.split('=')
    print(v)
    if v[0]=='None':
        return token,0,-1
    medium = None
    medium_coin = ['ETH', 'BTC']
    medium_currency = ['USD', 'EUR']
    medium_flag = 0
    if len(v)<2:
        r = v[0].split(' ')
        if r[1] in medium_coin:
            medium_flag=1
        elif r[1] in medium_currency:
            medium_flag=2
        try:
            return float(r[0].replace(',','')),r[1],medium_flag
        except ValueError as e:
            print(e)
            return token,0,-1
    token_flag = 0
    for i in v:
        if token in i:
            token_char = i
            token_flag = 1
            v.remove(i)
            price_char =v[0]
    if token_flag == 0:
        return token,0,-1

    token_char_num = re.findall("[^A-Z]*",token_char)[0]
    token_char_num = token_char_num.replace(',','')
    if not token_char_num:
        token_char_num = 1

    price_char_num = re.findall("[^A-Z]*",price_char)[0]
    price_char_num = price_char_num.replace(',','')
    if not price_char_num:
        price_char_num = 1
    try:
        rate = float(price_char_num)/float(token_char_num)
    except ValueError as e:
        print(e)
        rate=0


    for i in medium_coin:
        if i in price_char:
            medium = i
            medium_flag = 1

    if medium_flag == 0:
        for j in medium_currency:
            if j in price_char:
                medium = j
                medium_flag = 2

    if medium_flag ==0:
        medium = re.findall("[a-zA-Z]+",price_char)[0]
    return rate,medium,medium_flag


if __name__ == '__main__':
    print('Connecting database...')
    conn = pymysql.connect("121.41.55.91", "shixi", "shixi", "ico_coin", use_unicode=True, charset="utf8")
    cur = conn.cursor()
    cur.execute('SET NAMES utf8;')
    cur.execute('SET CHARACTER SET utf8;')
    cur.execute('SET character_set_connection=utf8;')
    while(True):
        token=get_token()
        print('Proccessing '+str(token))
        rate,medium,medium_flag = reform_price(token)
        status = price_comp(token,medium,rate,medium_flag)
        if status == -1:
            print('NOT FOUND')
        else:
            print('GATCHA')
