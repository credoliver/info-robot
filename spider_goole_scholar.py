# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import urllib2
import MySQLdb
from urlparse import *
import requests
import time
import re

sci_url = '.https.sci-hub.nz'
browser = webdriver.Chrome()

#拼接sci_hub查询url
def scihub_link(url):
    patterns = urlparse(url)
    return 'http://'+patterns.netloc+sci_url+patterns.path

def keywords_urls(keywords,count):
    urls = []
    start_page = 'https://scholar.google.com/scholar?&q='+keywords
    urls.append(urls)
    for i in range(1,count):
        urls.append('https://scholar.google.com/scholar?start='+str(i)+'0&q=PD-1&hl=en&as_sdt=0,5')
    return urls

def get_keyword():
    cur.execute("insert into keywords_scholar(key_word,dttm) (select key_word,dttm from keyword where key_word not in (select key_word from keywords_scholar))")
    try:
        cur.execute("update keywords_scholar set state='E' where dttm < date_sub(now(), interval 6 hour) and state='R'")
        cur.execute("select * from keywords_scholar where state='R'")
        keywords = cur.fetchall()
        keyword = ''
        if keywords:
            keyword = keywords[0][0]
        else:
            cur.execute("select * from keywords_scholar where isnull(state) or state='' order by dttm desc limit 1")
            keywords = cur.fetchall()
            if keywords:
                keyword = keywords[0][0]
            else:
                cur.execute("select * from keywords_scholar where state='Y'  and DATE_SUB(now(),INTERVAL 30 day)>dttm order by dttm limit 1")
                keywords = cur.fetchall()
                if keywords:
                    keyword = keywords[0][0]
                else:
                    cur.execute("select * from keywords_scholar where state='E'  and DATE_SUB(now(),INTERVAL 1 day)>dttm order by dttm limit 1")
                    keywords = cur.fetchall()
                    if keywords:
                        keyword = keywords[0][0]
                    else:
                        cur.execute("select * from keywords_schoalr where state='N'  and DATE_SUB(now(),INTERVAL 30 day)>dttm order by dttm limit 1")
                        keywords = cur.fetchall()
                        if keywords:
                            keyword = keywords[0][0]
        if keyword & len(keyword)>0:
            cur.execute("UPDATE keywords_scholar SET dttm=now(),state='R' WHERE key_word = '"+ keyword.replaceAll("'", "''").replaceAll("\\\\", "\\\\\\\\") + "' and (isnull(state) or state!='R' or (state='R' ))")

    except Exception, e:
        print e
    return keyword


def search_scihub(url):
    if '.pdf' in url:
        cur.execute("insert ignore pdfs_scholar(url,links,txt) values('%s','%s','%s')" % (url, url, ''))
        conn.commit()
        return 'Y'
    else:
        try:
            link = scihub_link(url).encode('utf-8')
            res_page = requests.get(link)
            res_html = BeautifulSoup(res_page.text,'html.parser')
            src = res_html.find_all('iframe')

            if src:
                cur.execute("insert ignore pdfs_scholar(url,links,txt) values('%s','%s','%s')" % (url, src[0]['src'], ''))
                conn.commit()
                return 'Y'
            else:
                return 'N'
        except Exception,e:
            print('Error at '+str(link))
            return 'N'

#输入搜索页面URL,返回该页面中文章的url,title和html
def articles_scholar(url):
    print('openning page '+str(url))
    browser.get(url)
    homepage_source = browser.page_source
    homepage = homepage_source.encode('utf-8')
    soup = BeautifulSoup(homepage, "html.parser")
    tag = soup.find_all('h3')
    print('dealing with articles...')
    count = 0
    for t in tag:
        print('dealing with '+str(count))
        item = BeautifulSoup(str(t),'html.parser')
        try:
            article_url = (item.a)['href'].encode('utf-8')
            article_url = MySQLdb.escape_string(article_url)
        except Exception,e:
            print(e)
            continue
        article_tittle = item.get_text().encode('utf-8')
        article_tittle = MySQLdb.escape_string(article_tittle)
        try:
            article_res = requests.get(article_url,timeout=(10,60))
            article_html = BeautifulSoup(article_res.text,'html.parser').encode('utf-8')
            article_html = MySQLdb.escape_string(article_html)
        except Exception,e:
            print('Can not reach article page')
            article_html = 'NULL'
            article_html = MySQLdb.escape_string(article_html)

        status = search_scihub(article_url)
        status = MySQLdb.escape_string(status)
        try:
            cur.execute("insert ignore articles_scholar(url,title,html,state) values('%s','%s','%s','%s')" % (article_url, article_tittle,article_html, status))
            conn.commit()
            count = count + 1
        except Exception, e:
            print(e)
    nextpage_tag = soup.find('td', attrs={"align": "left"})
    if nextpage_tag:
        nextpage_url = 'http://scholar.google.com' + nextpage_tag.a['href']
        articles_scholar(nextpage_url)
    else:
        return






if __name__ == '__main__':
    print('Connecting database...')
    conn = MySQLdb.connect(host="121.41.55.91 ", user="shixi", passwd="shixi", db="drugs")
    cur = conn.cursor()
    print('Database Connected.')
    keyword = get_keyword()
    while keyword:
        print('Processing keyword:'+keyword)
        home_page = 'https://scholar.google.com/scholar?hl=en&as_sdt=0%2C5&q='+keyword+'&btnG='
        articles_scholar(home_page)
        keyword = get_keyword()

