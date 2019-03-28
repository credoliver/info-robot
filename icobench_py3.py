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

reload(sys)

url = 'https://icobench.com/icos?filterBonus=&filterBounty=&filterTeam=&filterExpert=&filterSort=&filterCategory=all&filterRating=any&filterStatus=ended&filterCountry=any&filterRegistration=0&filterExcludeArea=none&filterPlatform=any&filterCurrency=any&filterTrading=any&s=&filterStartAfter=&filterEndBefore='
def changemonth(str):
    # month = str.lower()
    month = str.replace('Jan','01').replace('Feb','02')\
        .replace('Mar','03').replace('Apr','04').replace('May','05')\
        .replace('Jun','06').replace('Jul','07').replace('Aug','08')\
        .replace('Sep','09').replace('Oct','10').replace('Nov','11')\
        .replace('Dec','12')
    return month


def changedate(str):
    if len(str.split()) <3:
        return str
    day = str.split()[0]
    month = str.split()[1]
    year = str.split()[2]
    month = changemonth(month)
    date = year+'-'+month+'-'+day
    return date


def str_convert(str):
    lower = str.lower()
    str = lower.replace(' ', '_')
    str = str.replace('/', '_')
    str = str.replace('.', '_')
    return str

def run():
    # sys.setdefaultencoding('utf8')
    print('Connecting database...')
    try:
        conn = pymysql.connect("121.41.55.91", "shixi", "shixi", "ico_coin", use_unicode=True, charset="utf8")
        cur = conn.cursor()
        cur.execute('SET NAMES utf8;')
        cur.execute('SET CHARACTER SET utf8;')
        cur.execute('SET character_set_connection=utf8;')
        print('Database Connected.')
    except Exception as e:
        print(e)
        time.sleep(5)
        return
    error_pages = []
    total_count = 0
    cur_page_url = url
    while(cur_page_url):
        try:
            cur_page = requests.get(cur_page_url,timeout=60)
        except Exception as e:
            print(e)
            print('Error at page:'+str(cur_page_url))
            print('Sleep 10 seconds for another try.')
            time.sleep(10)
            continue

        cur_page_html = BeautifulSoup(cur_page.text,'html.parser')
        items = cur_page_html.find_all('tr')
        for i in range(1, len(items)):
            try:
                print('processing ' + str(i))
                ico_id_tag = items[i].find('a', attrs={"class": "name"})
                ico_url = 'https://icobench.com' + str(ico_id_tag['href'])
                ico_id = ico_id_tag['href'].split('/')[-1]

                content_tag = items[i].find(attrs={"class": "content"})
                content = pymysql.escape_string(str(content_tag.p))

                print(ico_url)
                temp_tag = items[i].find_all('td', attrs={"class": "rmv"})
                start_time = changedate(temp_tag[0].get_text())
                end_time = changedate(temp_tag[1].get_text())
                rate = temp_tag[2].get_text()
                try:
                    ico_page = requests.get(ico_url, timeout=60)
                except requests.exceptions.ConnectionError as e:
                    error_pages.append(ico_url)
                    print(e)
                    continue
                except requests.exceptions.ReadTimeout as e:
                    error_pages.append(ico_url)
                    time.sleep(10)
                    print(e)
                    continue

                ico_page_html = BeautifulSoup(ico_page.text, 'html.parser')
                profile_header = ico_page_html.find(attrs={"id": "profile_header"})
                profile_content = ico_page_html.find(attrs={"id": "profile_content"})

                '''name&business'''
                name_tag = profile_header.find(attrs={"class": "name"})
                ico_name = pymysql.escape_string(name_tag.h1.get_text())
                business = pymysql.escape_string(name_tag.h2.get_text())

                '''Image'''
                image_tag = profile_header.find(attrs={"class": "image"})
                img_url = 'https://icobench.com' + str(image_tag.img['src'])

                '''Abstract'''
                abstract = profile_header.find('p').get_text()
                abstract = pymysql.escape_string(str(abstract))

                '''Categories'''
                ctg_tag = profile_header.find(attrs={"class": "categories"})
                for child in ctg_tag.children:
                    if child:
                        try:
                            # ctg_url = 'https://icobench.com' + str(child['href'])
                            ctg_title = child['title']
                            cur.execute("insert ignore icobench_categories(id,category) values('%s','%s')" % (
                            ico_id, ctg_title))
                            conn.commit()
                        except Exception as e:
                            print(e)

                '''Distributions'''
                distribution = profile_header.find(attrs={"class": "distribution"})
                if distribution:
                    text_raw = distribution.get_text()
                    scores = re.findall(r"\d+\.?\d*", text_raw)
                    try:
                        ico_profile_score, team_score, vision_score, product_score = scores[0:4]
                    except ValueError as e:
                        print(e)

                else:
                    distribution = profile_header.find(attrs={"class": "distribution"})
                    if distribution:
                        text_raw = distribution.get_text()
                        scores = re.findall(r"\d+\.?\d*", text_raw)
                    try:
                        ico_profile_score, team_score, vision_score, product_score = scores[0:4]
                    except ValueError as e:
                        print(e)
                    else:
                        ico_profile_score, team_score, vision_score, product_score = None, None, None, None

                '''Financial Data'''
                fixed_data = ico_page_html.find(attrs={"class": "fixed_data"})
                fin_data = fixed_data.find(attrs={"class": "financial_data"})
                rows = fin_data.find_all(attrs={"class": "data_row"})
                financial_data = {}
                for row in rows:
                    key_raw = row.contents[1].get_text()
                    value_raw = row.contents[3].get_text()
                    key = str_convert(''.join(key_raw.split()))
                    value = str(''.join(value_raw.split()))
                    financial_data[key] = value
                try:
                    token = financial_data.get('token')
                    preico_price = financial_data.get('preicoprice')
                    price = financial_data.get('price')
                    bonus = financial_data.get('bonus')
                    bounty = financial_data.get('bounty')
                    platform = financial_data.get('platform')
                    accepting = financial_data.get('accepting')
                    min_inves = financial_data.get('minimuminvestment')
                    soft_cap = financial_data.get('softcap')
                    hard_cap = financial_data.get('hardcap')
                    country = financial_data.get('country')
                    whitelist_kyc = financial_data.get('whitelist_kyc')
                    res_areas = financial_data.get('restrictedareas')

                except Exception as e:
                    print('Keyerror')
                    print(e)

                '''Socials'''
                socials_tag = profile_header.find(attrs={"class": "socials"})
                if socials_tag:
                    url_tag = socials_tag.find(attrs={"class": "www"})
                else:
                    url_tag = None
                if url_tag:
                    original_url = url_tag['href'].split('?')[0]
                else:
                    original_url = None

                '''White Paper'''
                tabs_tag = profile_content.find(attrs={"class": "tabs"})
                tab = tabs_tag.find_all('a')
                if 'White paper' in tab[-1]:
                    whitepaper_url = tab[-1]['href']
                else:
                    whitepaper_url = None

                '''Kyc report'''
                kyc_info = profile_content.find(attrs={"class": "kyc_information"})
                kyc_info = str(kyc_info)
                kyc_info = pymysql.escape_string(kyc_info)
                '''About'''
                about_tag = ico_page_html.find(attrs={"id": "about"})
                about = about_tag.get_text()
                about = pymysql.escape_string(about)
                try:
                    cur.execute("replace into "
                                "icobench("
                                "id,ico,content,business,image,start,end,rate,ico_profile_rate,team_rate,"
                                "vision_rate,product_rate,ico_url,abstract,about,url,"
                                "token,preico_price,price,bonus,bounty,platform,"
                                "accepting,min_inves,soft_cap,hard_cap,country,"
                                "whitelist_kyc,res_areas,white_paper,ico_kyc_report)"
                                " values('%s','%s','%s','%s','%s','%s','%s',"
                                "'%s','%s','%s','%s','%s','%s','%s','%s','%s',"
                                "'%s','%s','%s','%s','%s','%s','%s',"
                                "'%s','%s','%s','%s','%s','%s','%s',"
                                "'%s')" %
                                (ico_id, ico_name, content, business, img_url, start_time, end_time, rate,
                                 ico_profile_score, team_score, vision_score,
                                 product_score, ico_url, abstract, about, original_url,
                                 token, preico_price, price, bonus, bounty, platform,
                                 accepting, min_inves, soft_cap, hard_cap, country,
                                 whitelist_kyc, res_areas, whitepaper_url, kyc_info
                                 ))
                    conn.commit()
                except Exception as e:
                    print(e)

                '''Team'''
                team = profile_content.find(attrs={"id": "team"})
                members = team.find_all(attrs={"class": "col_3"})
                for child in team.children:
                    try:
                        if child.name == 'h2' or child.name == 'h3':
                            flag = child.get_text().lower()
                        elif child.name == 'div':
                            for m in child.children:
                                tag = pymysql.escape_string(flag)
                                member_name = pymysql.escape_string(m.a['title'])
                                if m.h4:
                                    position = m.h4.get_text()
                                    position = pymysql.escape_string(position)
                                else:
                                    position = None
                                member_icourl = 'https://icobench.com' + m.a['href']
                                member_icourl = pymysql.escape_string(member_icourl)

                                pic_url_raw = m.find(attrs={"class": "image_background"})['style']
                                pic_url = 'https://icobench.com' + re.split("'", pic_url_raw)[1]
                                pic_url = pymysql.escape_string(pic_url)
                                suc_score_tag = m.find(attrs={"class": "icon_iss"})
                                suc_score = None
                                linkedin_tag = m.find(attrs={"class": "linkedin"})
                                if linkedin_tag:
                                    linkedin_url = linkedin_tag['href']
                                    linkedin_url = pymysql.escape_string(linkedin_url)
                                else:
                                    linkedin_url = None
                                if suc_score_tag:
                                    suc_score_str = suc_score_tag['data-tooltip']
                                    suc_score = re.split(":", suc_score_str)[1]
                                # print(member_name, position, member_icourl, pic_url, suc_score)
                                try:
                                    cur.execute("insert ignore icobench_team(ico,position,name,"
                                                "pic,url,linkedin_url,ico_suc_score,tittle) "
                                                "values('%s','%s','%s','%s','%s','%s','%s','%s')" %
                                                (ico_id, position, member_name, pic_url, member_icourl, linkedin_url,
                                                 suc_score, tag))
                                except Exception as e:
                                    print("insert ignore icobench_team(ico,position,name,"
                                          "pic,url,linkedin_url,ico_suc_score,tittle) "
                                          "values('%s','%s','%s','%s','%s','%s','%s','%s')" %
                                          (ico_id, position, member_name, pic_url, member_icourl, linkedin_url,
                                           suc_score, tag))
                                    print(e)
                    except AttributeError as e:
                        continue
                conn.commit()

                '''Milestone'''
                try:
                    milestone_tag = profile_content.find(attrs={"id": "milestones"})
                    milestones = milestone_tag.find_all(attrs={"class": "row"})
                except AttributeError as e:
                    print(e)
                try:
                    index = 0
                    for m in milestones:
                        date = m.find(attrs={"class": "condition"}).get_text()
                        date = pymysql.escape_string(date)
                        event = m.find('p').get_text()
                        event = pymysql.escape_string(event)
                        cur.execute(
                            "insert ignore icobench_milestones(ico,list,date,events) values('%s','%s','%s','%s')" % (
                                ico_id, index, date, event))
                        conn.commit()
                        index = index + 1
                except Exception as e:
                    print(e)

                '''Financial'''
                fin = profile_content.find(attrs={"id": "financial"})
                boxes = fin.find(attrs={"class": "box"})
                rows = boxes.find_all(attrs={"class": "row"})
                financial = {}
                key = None
                value = None
                for row in rows:
                    for r in row.children:
                        if isinstance(r, bs4.element.Tag):
                            if 'class' in r.attrs:
                                try:
                                    if r['class'][0] == 'label':
                                        key = str_convert(r.get_text())
                                    elif r['class'][0] == 'value':
                                        value = r.get_text()
                                except AttributeError as e:
                                    continue
                            elif r.name == 'h4':
                                key = str_convert(r.get_text())
                                value = row.find(attrs={"class": "bonus_text"}).table
                    financial[key] = value
                try:
                    ftoken = financial.get('token')
                    fplatform = financial.get('platform')
                    ftype = financial.get('type')
                    fbonus = financial.get('bonus')
                    fprice_in_ico = financial.get('price_in_ico')
                    ftoken_for_sale = financial.get('tokens_for_sale')
                    faccepting = financial.get('accepting')
                    fdistributed_in_ico = financial.get('distributed_in_ico')
                    fmin_inves = financial.get('minimum_investment')
                    fsoft_cap = financial.get('soft_cap')
                    fhard_cap = financial.get('hard_cap')
                except Exception as e:
                    print(e)
                try:
                    cur.execute("insert ignore icobench_financial(ico,token,platform,type,price_in_ico,"
                                "bonus,token_for_sale,accepting,distributed_in_ico,soft_cap,hard_cap,min_investment) "
                                "values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
                                % (ico_id, ftoken, fplatform, ftype, fprice_in_ico, fbonus, ftoken_for_sale, faccepting,
                                   fdistributed_in_ico, fsoft_cap,
                                   fhard_cap, fmin_inves))
                    conn.commit()
                except Exception as e:
                    print(e)

                '''Ratings'''
                ratings_tag = profile_content.find(attrs={"id": "ratings"})
                ratings = ratings_tag.find_all(attrs={"class": "row"})
                for r in ratings:
                    rater_name = r.find(attrs={"class": "name"}).get_text()
                    rates_tag = r.find_all(attrs={"class": "col_3"})
                    if len(rates_tag) < 1:
                        rates_tag = r.find(attrs={"class": "col_1"})
                        bot_rate = re.findall("\d{1}\.?\d{0,1}", rates_tag.get_text())[0]
                        team_rate = bot_rate
                        vision_rate = bot_rate
                        product_rate = bot_rate
                    else:
                        team_rate = re.findall("\d", rates_tag[0].get_text())[0]
                        vision_rate = re.findall("\d", rates_tag[1].get_text())[0]
                        product_rate = re.findall("\d", rates_tag[2].get_text())[0]
                    weight_tag = r.find(attrs={"class": "distribution"})
                    weight = re.findall("\d{1,2}", weight_tag.get_text())[0] + '%'
                    cur.execute("insert ignore icobench_ratings(ico,name,team_rate,vision_rate,product_rate,weight)"
                                "values('%s','%s','%s','%s','%s','%s')"
                                % (ico_id, rater_name, team_rate, vision_rate, product_rate, weight))
                    conn.commit()
                total_count = total_count + 1
                print('total ' + str(total_count) + ' icos downloaded')
                print('Finish ,now sleep')
                time.sleep(5)
            except Exception as e:
                print(e)
                time.sleep(15)
                continue
        next_url_tag = cur_page_html.find(attrs={"class":"next"})
        try:
            next_url = 'https://icobench.com'+next_url_tag['href']
        except KeyError as e:
            next_url=None
            print('Finished.')
        cur_page_url = next_url

    cur.close()
    conn.close()
    return
if __name__ == '__main__':
    while(True):
        run()
        print('*************************************************')
        print('***Task Finished, wait 12h for the next round.***')
        print('*************************************************')
        time_now = time.time()
        time_local = time.localtime(time_now)
        dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
        print('Finish time:'+str(dt))
        time_next = time_now+43200
        time_next_local = time.localtime(time_next)
        dt = time.strftime("%Y-%m-%d %H:%M:%S", time_next_local)
        print('Next round start time:'+str(dt))

        time.sleep(43200)

