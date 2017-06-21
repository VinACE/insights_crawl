
from datetime import datetime
from django.core.files import File
import glob, os
import pickle
import requests
import urllib
from urllib.parse import urlparse
import re
from requests_ntlm import HttpNtlmAuth
from pandas import DataFrame
from bs4 import BeautifulSoup

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.connections import connections
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import bulk

import app.models as models
import app.crawl as crawl


def put_settings(obj):
    indices_client = IndicesClient(models.client)
    index_name = obj._meta.es_index_name
    indices_client.close(index=index_name)
    kwargs = { "analysis": {
        "analyzer": {
            "default": {"tokenizer": "standard", "filter": ["synonym"]},
            "keepwords": {"tokenizer": "standard", "filter": ["keepwords"]},
            },
        "filter": {
            "synonym": {"type": "synonym", "synonyms_path": "synonym.txt"},
            "keepwords": {"type": "keep", "keep_words_path": "keepwords.txt"},
            }
        }
    }
    indices_client.put_settings(index=index_name, body=kwargs)
    indices_client.open(index=index_name)


def create_index_pi():
#   indices_client = IndicesClient(client=settings.ES_HOSTS)
    indices_client = IndicesClient(models.client)
    index_name = models.Review._meta.es_index_name
    if indices_client.exists(index_name):
        indices_client.delete(index=index_name)
    indices_client.create(index=index_name)
    indices_client.put_mapping(
        doc_type=models.Review._meta.es_type_name,
        body=models.Review._meta.es_mapping,
        index=index_name
    )

def create_index_mi():
    indices_client = IndicesClient(models.client)
    index_name = models.PostMap._meta.es_index_name
    if indices_client.exists(index_name):
        indices_client.delete(index=index_name)
    indices_client.create(index=index_name)
    indices_client.put_mapping(
        doc_type=models.PostMap._meta.es_type_name,
        body=models.PostMap._meta.es_mapping,
        index=index_name
    )


def create_index_si_sites():
    indices_client = IndicesClient(models.client)
    index_name = models.PageMap._meta.es_index_name
    if indices_client.exists(index_name):
        indices_client.delete(index=index_name)
    indices_client.create(index=index_name)
    indices_client.put_mapping(
        doc_type=models.PageMap._meta.es_type_name,
        body=models.PageMap._meta.es_mapping,
        index=index_name
    )


def create_index_mi_feedly():
    indices_client = IndicesClient(models.client)
    index_name = models.FeedlyMap._meta.es_index_name
    if indices_client.exists(index_name):
        indices_client.delete(index=index_name)
    indices_client.create(index=index_name)
    #put_settings(models.FeedlyMap)
    indices_client.put_mapping(
        doc_type=models.FeedlyMap._meta.es_type_name,
        body=models.FeedlyMap._meta.es_mapping,
        index=index_name
    )

def create_index_scentemotion():
    indices_client = IndicesClient(models.client)
    index_name = models.ScentemotionMap._meta.es_index_name
    if indices_client.exists(index_name):
        indices_client.delete(index=index_name)
    indices_client.create(index=index_name)
    #put_settings(models.ScentemotionMap)
    indices_client.put_mapping(
        doc_type=models.ScentemotionMap._meta.es_type_name,
        body=models.ScentemotionMap._meta.es_mapping,
        index=index_name
    )

def create_index_studies():
    indices_client = IndicesClient(models.client)
    index_name = models.StudiesMap._meta.es_index_name
    if indices_client.exists(index_name):
        indices_client.delete(index=index_name)
    indices_client.create(index=index_name)
    indices_client.put_mapping(
        doc_type=models.StudiesMap._meta.es_type_name,
        body=models.StudiesMap._meta.es_mapping,
        index=index_name
    )

def create_index_survey():
    indices_client = IndicesClient(models.client)
    index_name = models.SurveyMap._meta.es_index_name
    if indices_client.exists(index_name):
        indices_client.delete(index=index_name)
    indices_client.create(index=index_name)
    #put_settings(models.ScentemotionMap)
    indices_client.put_mapping(
        doc_type=models.SurveyMap._meta.es_type_name,
        body=models.SurveyMap._meta.es_mapping,
        index=index_name
    )


def create_index_elastic(index_choices):
    for index_choice in index_choices:
        if index_choice == 'pi':
            create_index_pi()
        elif index_choice == 'mi':
            create_index_mi()
        elif index_choice == 'si_sites':
            create_index_si_sites()
        elif index_choice == 'feedly':
            create_index_mi_feedly()
        elif index_choice == 'scentemotion':
            create_index_scentemotion()
        elif index_choice == 'studies':
            create_index_studies()
        elif index_choice == 'survey':
            create_index_survey()
            


def create_analyzer(index_choices):
    for index_choice in index_choices:
        if index_choice == 'pi':
            put_settings(models.Review)
        elif index_choice == 'mi':
            put_settings(models.PostMap)
        elif index_choice == 'si_sites':
            put_settings(models.PageMap)
        elif index_choice == 'feedly':
            put_settings(models.FeedlyMap)

def export_opml(index_choices, opml_filename):
    status = True
    for index_choice in index_choices:
        if index_choice == 'feedly':
            status = crawl.export_opml_feedly(opml_filename)
    return status

def import_opml(index_choices, opml_filename):
    status = True
    for index_choice in index_choices:
        if index_choice == 'feedly':
            status = crawl.import_opml_feedly(opml_filename)
    return status



def read_keywords(index_choices, keyword_filename):
    status = True
    for index_choice in index_choices:
        if index_choice == 'feedly':
            models.search_keywords[index_choice] = []
            keywords_input = ''
            keyword_file = 'data/' + keyword_filename
            try:
                file = open(keyword_file, 'r')
                pyfile = File(file)
                for line in pyfile:
                    keyword = line.rstrip('\n')
                    models.search_keywords[index_choice].append(keyword)
                    if keyword.count(' ') > 0:
                        keyword = '"' + keyword + '"'
                    if keywords_input == '':
                        keywords_input = keyword
                    else:
                        keywords_input = keywords_input + ',' + keyword
                pyfile.close()
            except:
                return False
            #models.FeedlySeekerView.facets_keyword[0].read_keywords = keywords_input

    return True
