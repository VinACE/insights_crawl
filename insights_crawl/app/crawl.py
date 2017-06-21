
from datetime import datetime
from datetime import time
from datetime import timedelta
from django.core.files import File
import glob, os
import pickle
import urllib
import requests
from urllib.parse import urlparse
import re
from requests_ntlm import HttpNtlmAuth
from pandas import Series, DataFrame
import pandas as pd
from bs4 import BeautifulSoup

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.connections import connections
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import bulk

import app.models as models
import app.elastic as elastic
import app.survey as survey

si_sites = {
    'gci'   : {
        'site_url'  : 'http://www.gcimagazine.com/',
        'sub_sites' : {
            'gci'   : 'http://www.gcimagazine.com/'},
        },
    }


class Crawler:
    site_name = ''
    pages = set()
    bulk_data = []
    nrpages = 50

    def __init__(self, site, nrpages):
        self.site = site
        self.nrpages = nrpages

    # read the content of a page into BeautifulSoup
    def read_page(self, url):
        try:
            print("read_page: scraping url ", url)
            html = urllib.request.urlopen(url)
            bs = BeautifulSoup(html.read(), "lxml")
            [script.decompose() for script in bs("script")]
        except:
            print("Scrape: could not open url ", url)
        return bs

    # Step though all summery pages (Next, Pagination) and from each summary page get all the link refering to the articles
    def get_pagination_links(self, sub_site):
        include_url = urlparse(sub_site).scheme+"://"+urlparse(sub_site).netloc
        links = set()
        url = sub_site
        page_nr = 0
        page_size = 10
        link_count = 1
        links.add(sub_site)
        return links

    # get all the links that point within this site
    def get_internal_links(self, url, bs):
        include_url = urlparse(url).scheme+"://"+urlparse(url).netloc
        links = set()
        link_count = 0
        for link_tag in bs.findAll("a", href=re.compile("^(/|.*"+include_url+")")) and link_count < self.nrpages:
            if link_tag.attrs['href'] is not None:
                if link_tag.attrs['href'] not in links:
                    if link_tag.attrs['href'].startswith('/'):
                        link = include_url+link_tag.attrs['href']
                    else:
                        link = link_tag.attrs['href']
                    links.add(link)
                    link_count = link_count + 1
        return links

    # get all the links that point outside this site
    def get_external_links(self, url, bs):
        include_url = urlparse(url).scheme+"://"+urlparse(url).netloc
        links = set()
        for link_tag in bs.findAll("a", href=re.compile("^(/|.*"+include_url+")")):
            if link_tag.attrs['href'] is not None:
                if link_tag.attrs['href'] not in links:
                    if link_tag.attrs['href'].startswith('/'):
                        links.append(include_url+link_tag.attrs['href'])
                    else:
                        link_tag.append(link.attrs['href'])
        return links

    # for this page (url) scrape its context and map this to the elasticsearch record (pagemap)
    def scrape_page_map(self, sub_site, url, bs):
        id = url
        site_url = urlparse(url).netloc.split('.')[1]
        sub_site_url = urlparse(url).path.split('/')
        sub_site_name = '-'.join(sub_site[1:-1])
        if sub_site_name == '':
            sub_site_name = 'Home'
        pagemap             = models.PageMap()

        pagemap.page_id     = id
        pagemap.site        = self.site
        pagemap.sub_site    = sub_site
        pagemap.url         = url
        pagemap.section     = ''

        # get posted date
        try:
            pagemap.posted_date = datetime.today()
        except:
            pass

        # get page
        try:
            pagemap.page        = bs.get_text()
        except:
            pass

        # get title
        try:
            if bs.title != None:
                pagemap.title   = bs.title.text
            else:
                pagemap.title   = ''
        except:
            pass

        data = elastic.convert_for_bulk(pagemap, 'update')
        return data

def crawl_si_site(site_choice, nrpages):
    crawler = Crawler (site_choice, nrpages)
    si_site = si_sites[site_choice]
    sub_sites = si_site.sub_sites
    site_url = si_site.site_url
           
    for sub_site, sub_site_url in sub_sites.items():
        bs = crawler.read_page(sub_site_url)
        links = crawler.get_internal_links(sub_site_url, bs)        
        for link in links:
             bs = crawler.read_page(link)
             apf.pages.add(link)
             data = apf.scrape_page_map(sub_site, link, bs)
             apf.bulk_data.append(data)
    
    bulk(models.client, actions=apf.bulk_data, stats_only=True)


#****************************** APF Crawler *****************************************

class AFPCrawler(Crawler):

    def get_pagination_links(self, sub_site):
        include_url = urlparse(sub_site).scheme+"://"+urlparse(sub_site).netloc
        links = set()
        url = sub_site
        page_nr = 0
        page_size = 10
        link_count = 0
        while url != None and link_count < self.nrpages:
            bs = self.read_page(url)
            blog_posts_tag = bs.find("div", class_="blog-posts")
            for link_tag in blog_posts_tag.findAll("a", href=re.compile("^(/|.*"+include_url+")")):
                if link_tag.attrs['href'] is not None:
                    if link_tag.attrs['href'] not in links:
                        if link_tag.attrs['href'].startswith('/'):
                            link = include_url+link_tag.attrs['href']
                        else:
                            link = link_tag.attrs['href']
                        links.add(link)
                        link_count = link_count + 1
            navigation_tag = bs.find("nav", class_="nav-below")
            if navigation_tag != None:
                next_tag = navigation_tag.find("span", class_="nav-next")
                if next_tag != None:
                    next_url = next_tag.parent.attrs['href']
                else:
                    next_url = None
            url = next_url
        return links


    def scrape_page_map(self, sub_site, url, bs):
        id = url
        pagemap             = models.PageMap()
        pagemap.page_id     = id
        pagemap.site        = self.site
        pagemap.sub_site    = sub_site
        pagemap.url         = url

        # get posted date
        # <span class="entry-date">May 23, 2017</span>
        try:
            pagemap.posted_date = datetime.today()
            entry_date_tag = bs.find("span", class_="entry-date")
            published = entry_date_tag.text
            pagemap.posted_date = datetime.strptime(published, '%B %d, %Y')
        except:
            pass
        #try:
        #    box_1_tag = bs.find("div", class_="box_1")
        #    product_info_bar_tag = box_1_tag.find("div", class_="product_info_bar")
        #    published = re.search(r'([0-9]{2}-[a-z,A-Z]{3}-[0-9]{4})', product_info_bar.text, re.MULTILINE)
        #    pagemap.posted_date = datetime.strptime(published.group(0), '%d-%b-%Y')
        #except:
        #    pass

        # get page
        # <section class="entry-content">
        try:
            pagemap.page = bs.get_text()
            entry_content_tag = bs.find("section", class_="entry-content")
            pagemap.page = entry_content_tag.text
        except:
            pass
        # get title
        # <h1 class="entry-title"></h1>  text
        try:
            if bs.title != None:
                pagemap.title   = bs.title.text
            else:
                pagemap.title   = ''
            entry_title_tag = bs.find("h1", class_="entry-title")
            pagemap.title = entry_title_tag.text
        except:
            pass
        # get section
        try:
            pagemap.section = sub_site
        except:
            pass

        data = elastic.convert_for_bulk(pagemap, 'update')
        return data


def crawl_apf(scrape_choices, nrpages):
    apf = AFPCrawler ('APF', nrpages)
    sub_sites = {}
    site_url = 'https://apf.org/'
    for scrape_choice in scrape_choices:
        if scrape_choice == 'blog':
            sub_sites['blog'] = site_url + '/blog'
        if scrape_choice == 'publications':
            sub_sites['blog'] = site_url + '/publications'
           
    for sub_site, sub_site_url in sub_sites.items():
        links = apf.get_pagination_links(sub_site_url)        
        for link in links:
             bs = apf.read_page(link)
             apf.pages.add(link)
             data = apf.scrape_page_map(sub_site, link, bs)
             apf.bulk_data.append(data)
    
    bulk(models.client, actions=apf.bulk_data, stats_only=True)


#****************************** Cosmetics Crawler *****************************************

class CosmeticsCrawler(Crawler):

    def get_pagination_links(self, sub_site):
        include_url = urlparse(sub_site).scheme+"://"+urlparse(sub_site).netloc
        links = set()
        url = sub_site
        page_nr = 0
        page_size = 10
        link_count = 0
        while url != None and link_count < self.nrpages:
            bs = self.read_page(url)
            box_1_tag = bs.find("div", class_="box_1")
            for link_tag in box_1_tag.findAll("a", href=re.compile("^(/|.*"+include_url+")")):
                if link_tag.attrs['href'] is not None:
                    if link_tag.attrs['href'] not in links:
                        if link_tag.attrs['href'].startswith('/'):
                            link = include_url+link_tag.attrs['href']
                        else:
                            link = link_tag.attrs['href']
                        links.add(link)
                        link_count = link_count + 1
            result_count_tag = bs.find("span", class_="result_count")
            if result_count_tag != None:
                result_count_list = result_count_tag.text.split()
                result_count = int(float(result_count_list[4]))
            else:
                result_count = page_size
            navigation_tag = bs.find(id="navigation")
            if navigation_tag != None:
                next_tag = navigation_tag.find("span", class_="next")
                if next_tag != None:
                    next_url = include_url + next_tag.find("a").attrs['href']
                else:
                    next_url = None
            else:
                page_nr = page_nr + 1
                if page_nr * page_size > result_count:
                    next_url = None
                else:
                    next_url = sub_site + '/(offset)/{}'.format(page_nr)
            url = next_url
        return links


    def scrape_page_map(self, sub_site, url, bs):
        id = url
        pagemap             = models.PageMap()
        pagemap.page_id     = id
        pagemap.site        = self.site
        pagemap.sub_site    = sub_site
        pagemap.url         = url

        # get posted date
        try:
            pagemap.posted_date = datetime.today()
            author_info_tag = bs.find("div", class_="author_info")
            published = author_info_tag.find('p', class_='date').text
            pagemap.posted_date = datetime.strptime(published, '%d-%b-%Y')
        except:
            pass
        try:
            box_1_tag = bs.find("div", class_="box_1")
            product_info_bar_tag = box_1_tag.find("div", class_="product_info_bar")
            published = re.search(r'([0-9]{2}-[a-z,A-Z]{3}-[0-9]{4})', product_info_bar.text, re.MULTILINE)
            pagemap.posted_date = datetime.strptime(published.group(0), '%d-%b-%Y')
        except:
            pass
        # get page
        try:
            pagemap.page        = bs.get_text()
            box_1_tag = bs.find("div", class_="box_1")
            pagemap.page = box_1_tag.text
            product_main_text_tag = box_1_tag.find("div", class_="product_main_text")
            if product_main_text_tag != None:
                pagemap.page = product_main_text_tag.text
            else:
                story_tag = box_1_tag.find("div", class_="story")
                pagemap.page = story_tag.text
        except:
            pass
        # get title
        try:
            if bs.title != None:
                pagemap.title   = bs.title.text
            else:
                pagemap.title   = ''
            box_1_tag = bs.find("div", class_="box_1")
            pagemap.title = box_1_tag.find("h1").text
        except:
            pass
        # get section
        try:
            box_2_tag = bs.find("div", class_="box_2")
            pagemap.section = box_2_tag.text.strip(' \t\n\r')
        except:
            pass

        data = elastic.convert_for_bulk(pagemap, 'update')
        return data


def crawl_cosmetic(scrape_choices, nrpages):
    cosmetic = CosmeticsCrawler('Cosmetics', nrpages)
    sub_sites = {}
    if len(scrape_choices) == 0:
        sub_sites.add(site)
#   for site in ['http://www.cosmeticsdesign.com/', 'http://www.cosmeticsdesign-europe.com/', 'http://www.cosmeticsdesign-asia.com/']:
    for site_url in ['http://www.cosmeticsdesign.com/']:
        for scrape_choice in scrape_choices:
            if scrape_choice == 'product':
                sub_sites['Skin-care'] = site_url + '/Product-Categories/Skin-Care'
                sub_sites['Hair-care'] = site_url +'/Product-Categories/Hair-Care'
            if scrape_choice == 'market':
                sub_sites['Market-Trends'] = site_url + '/Market-Trends'
                sub_sites['Brand-Innovation']= site_url +'/Brand-Innovation'

    for sub_site, sub_site_url in sub_sites.items():
        links = cosmetic.get_pagination_links(sub_site_url)
        for link in links:
            bs = cosmetic.read_page(link)
            cosmetic.pages.add(link)
            data = cosmetic.scrape_page_map(sub_site, link, bs)
            cosmetic.bulk_data.append(data)

    bulk(models.client, actions=cosmetic.bulk_data, stats_only=True)

#
# FEEDLY
#

def crawl_feedly(from_date, rss_field):
    global headers

    bulk_data = []
    today = datetime.now()
    days = timedelta(days=31)
    yesterday = today - days
    s = yesterday.timestamp()
    t = time(0, 0)
    dt = datetime.combine(from_date, t)
    s = dt.timestamp()
    #datetime.datetime.fromtimestamp(s).strftime('%c')
    ms = s * 1000
    newerthan = "{:.0f}".format(ms)
    headers = {
        #sjaak.waarts@gmail.com (expires on 2017-07-20)
        "Authorization" : "A2JxorrfeTBQbMUsDIU3_zexSwY8191e3P9EvewYowjfbhKwOgHk84ErlXAWXpucZ_McfTDHLZN6yLxWqxgjWM8Upp1c-6Nb_RpZd0jWA9mJkVLN1JTETefaVNZtZqzTGTf8_qeT2ZE8z6Bf4LqLOUfQaQH2-jj8XIaxAyWMZ5BDRtfpgwVYrEEM2ii5KXnMJZxGNEvcqAV4Dke_subaM-wlnC8N63g:feedlydev"
        }

    params_streams = {
#       "count"     : "100",
        "count"     : "1000",
        "ranked"    : "newest",
        "unreadOnly": "false",
        "newerThan" : newerthan
        }
    #url = "http://cloud.feedly.com/v3/profile"
    #r = requests.get(url, headers=headers)
    url = "http://cloud.feedly.com/v3/subscriptions"
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        return False
    feeds = r.json()
    for feed in feeds:
        feed_id = feed['id']
        feed_title = feed['title'].encode("ascii", 'replace')
        feed_category = feed['categories'][0]['label']
        print("Scrape: scraping feed ", feed_title)
        if rss_field == '' or feed_category == rss_field:
            url = "http://cloud.feedly.com//v3/streams/contents"
            params_streams['streamId'] = feed_id
            r = requests.get(url, headers=headers, params=params_streams)
            stream = r.json()
            for entry in stream['items']:
                feedlymap = models.FeedlyMap()
                feedlymap.post_id = entry['id']
                feedlymap.published_date = datetime.fromtimestamp(entry['published']/1000)
                feedlymap.category = feed_category
                feedlymap.feed = feed_title
                if 'topics' in feed:
                    feedlymap.feed_topics = feed['topics']
                if 'keywords' in entry:
                    feedlymap.body_topics = entry['keywords']
                feedlymap.title = entry['title']
                if 'canonicalUrl' in entry:
                    feedlymap.url = entry['canonicalUrl']
                else:
                    n = entry['originId'].find('http')
                    feedlymap.url = entry['originId'][n:]
                feedlymap.post_id = feedlymap.url
                if 'summary' in entry:
                    bs = BeautifulSoup(entry['summary']['content'],  "lxml") # in case of RSS feed
                if 'content' in entry:
                    bs = BeautifulSoup(entry['content']['content'], "lxml") # in case of Google News feed
                feedlymap.body = bs.get_text().encode("ascii", 'replace')
                data = elastic.convert_for_bulk(feedlymap, 'update')
                bulk_data.append(data)

    bulk(models.client, actions=bulk_data, stats_only=True)
    return True

def export_opml_feedly(opml_filename):
    global headers

    url = "http://cloud.feedly.com/v3/opml"
    r = requests.get(url, headers=headers)
    xml = r.content

    opml_file = 'data/' + opml_filename + '_opml.txt'
    try:
        file = open(opml_file, 'wb')
        pyfile = File(file)
        pyfile.write(xml)
        pyfile.close()
        return True
    except:
        return False


def import_opml_feedly(opml_filename):
    global headers

    opml_file = 'data/' + opml_filename + '_opml.txt'
    try:
        file = open(opml_file, 'rb')
        pyfile = File(file)
        xml = pyfile.read()
        pyfile.close()
    except:
        return False

    url = "http://cloud.feedly.com/v3/opml"
    h2 = headers
    h2['Content-Type'] = 'application/xml'
    r = requests.post(url, headers=headers, data=xml)
    
    return True


def crawl_studies_facts(survey_field, facts_d):
    bulk_data = []
    count = 0
    total_count = 0
    facts_df = DataFrame.from_dict(facts_d, orient='index')
    facts_df['blindcode'] = [ix[0] for ix in facts_df.index]
    facts_df['fact'] = [ix[1] for ix in facts_df.index]
    facts_df['answer'] = [ix[2] for ix in facts_df.index]

    for blindcode, facts_blindcode_df in facts_df.groupby(facts_df['blindcode']):
        se = models.StudiesMap()
        se.cft_id = blindcode
        se.dataset = survey_field
        se.ingr_name = blindcode
        se.IPC = blindcode
        percentile = {}

        for idx, fact_s in facts_blindcode_df.iterrows():
            fact = fact_s['fact']
            answer = fact_s['answer']
            #se.supplier = "CI"
            #se.olfactive = cft_s.olfactive
            #se.region = cft_s.region
            #se.review = cft_s.review
            #se.dilution = cft_s.dilution
            #se.intensity = cft_s.intensity

            if fact not in percentile.keys():
                percentile[fact] = []
            val = answer
            prc = fact_s[0]
            if prc > 0:
                percentile[fact].append((val, prc))

        for fact in percentile.keys():
            if fact == 'emotion':
                se.emotion = percentile[fact]
            if fact == 'suitable_stage':
                se.suitable_stage = percentile[fact]
            if fact == 'hedonics':
                se.hedonics = percentile[fact]
            if fact == 'freshness':
                se.freshness = percentile[fact]

        data = elastic.convert_for_bulk(se, 'update')
        bulk_data.append(data)
        count = count + 1
        if count > 100:
            bulk(models.client, actions=bulk_data, stats_only=True)
            total_count = total_count + count
            print("crawl_studies_facts: written another batch, total written {0:d}".format(total_count))
            bulk_data = []
            count = 1

    bulk(models.client, actions=bulk_data, stats_only=True)
    pass

def crawl_scentemotion(cft_filename):
    ml_file = 'data/' + cft_filename
    cft_df = pd.read_csv(ml_file, sep=';', encoding='ISO-8859-1', low_memory=False)
    cft_df.fillna(0, inplace=True)
    cft_df.index = cft_df['cft_id']
    bulk_data = []
    count = 0
    total_count = 0
    for cft_id, cft_s in cft_df.iterrows():
        se = models.ScentemotionMap()
        se.cft_id = cft_id
        se.dataset = "ingredients"
        se.ingr_name = cft_s.ingr_name
        se.IPC = cft_s.IPC
        se.supplier = cft_s.supplier
        se.olfactive = cft_s.olfactive
        se.region = cft_s.region
        se.review = cft_s.review
        se.dilution = cft_s.dilution
        se.intensity = cft_s.intensity

        percentile = {}
        for col in cft_s.index:
            col_l = col.split("_", 1)
            fct = col_l[0]
            if fct not in ["mood", "smell", "negative", "descriptor", "color", "texture"]:
                continue
            if fct not in percentile.keys():
                percentile[fct] = []
            val = col_l[1]
            prc = cft_s[col]
            if prc > 0:
                #percentile[fct].append((val, "{0:4.2f}".format(prc)))
                percentile[fct].append((val, prc))

        se.mood = percentile["mood"]
        se.smell = percentile["smell"]
        se.negative = percentile["negative"]
        se.descriptor = percentile["descriptor"]
        se.color = percentile["color"]
        se.texture = percentile["texture"]

        data = elastic.convert_for_bulk(se, 'update')
        bulk_data.append(data)
        count = count + 1
        if count > 100:
            bulk(models.client, actions=bulk_data, stats_only=True)
            total_count = total_count + count
            print("crawl_scentemotion: written another batch, total written {0:d}".format(total_count))
            bulk_data = []
            count = 1

    bulk(models.client, actions=bulk_data, stats_only=True)
    pass

def map_survey(survey_filename):
    ml_file = 'data/' + survey_filename
    survey_df = pd.read_csv(ml_file, sep=';', encoding='ISO-8859-1', low_memory=False)
    survey_df.fillna(0, inplace=True)
    field_map , col_map = survey.map_columns(survey_df.columns)
    return col_map

def crawl_survey(survey_filename):
    ml_file = 'data/' + survey_filename
    survey_df = pd.read_csv(ml_file, sep=';', encoding='ISO-8859-1', low_memory=False)
    survey_df.fillna(0, inplace=True)
    # col_map[column]: (field, question, answer, dashboard)
    # field_map[field]: [question=0, answer=1, column=2)]
    field_map , col_map = survey.map_columns(survey_df.columns)
    survey_df.index = survey_df[field_map['resp_id'][0][2]]
    bulk_data = []
    count = 0
    total_count = 0
    for resp_id, survey_s in survey_df.iterrows():
        sl = models.SurveyMap()
        resp_id = survey.answer_value_to_string(survey_s[field_map['resp_id'][0][2]])
        blindcode = survey.answer_value_to_string(survey_s[field_map['blindcode'][0][2]])
        sl.resp_id = resp_id+"_"+blindcode
        sl.survey  = survey_filename
        sl.children = {}
        sl.concept = {}
        sl.emotion = {}
        sl.fragrattr = {}
        sl.mood = {}
        sl.smell = {}
        sl.suitable_product = {}
        sl.suitable_stage = {}
        sl.question = {}
        for field, maps in field_map.items():
            # resp_id is the unique id of the record, this is already set above
            if field == 'resp_id':
                continue
            # map: 0=answer, 1=column
            map = maps[0]
            answer_value = survey_s[map[2]]
            answer_value = survey.answer_value_to_string(answer_value)
            answer_value = survey.answer_value_encode(map[1], answer_value)
            # column mapping, no question
            if map[0] == None:
                # in case of multiple mapping search for the column that has a value
                for ix in range(1, len(maps)):
                    map = maps[ix]
                    answer_value_2 = survey_s[map[2]]
                    answer_value_2 = survey.answer_value_to_string(answer_value_2)
                    if (field == 'blindcode'):
                        answer_value = answer_value + '-' + answer_value_2[:3]
                    else:
                        if len(answer_value_2) > len(answer_value):
                            answer_value = answer_value_2
                setattr(sl, field, answer_value)
            # answer mapping
            else:
                setattr(sl, field, {map[1]: answer_value})
                attr = getattr(sl, field)
                for ix in range(1, len(maps)):
                    map = maps[ix]
                    answer_value = survey_s[map[2]]
                    answer_value = survey.answer_value_to_string(answer_value)
                    answer_value = survey.answer_value_encode(map[1], answer_value)
                    attr[map[1]] = answer_value
                    #attr.append({map[1]: answer_value})

        data = elastic.convert_for_bulk(sl, 'update')
        bulk_data.append(data)
        count = count + 1
        if count > 100:
            bulk(models.client, actions=bulk_data, stats_only=True)
            total_count = total_count + count
            print("crawl_survey: written another batch, total written {0:d}".format(total_count))
            bulk_data = []
            count = 1
            #break

    bulk(models.client, actions=bulk_data, stats_only=True)
    pass






