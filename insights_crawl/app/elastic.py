
from datetime import datetime
import requests
from requests_ntlm import HttpNtlmAuth
from requests.auth import HTTPBasicAuth

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.connections import connections

import app.models as models

#from elasticsearch_dsl import DocType, String, Date, Double, Long, Integer

#class Account(DocType):
#    title = String(analyzer='snowball', fields={'raw': String(index='not_analyzed')})
#    body = String(analyzer='snowball')
#    tags = String(index='not_analyzed')
#    published_from = Date()
#    account_number = Long()
#    address = String()
#    age = Long()
#    balance = Long()
#    city = String()
#    employer = String()
#    firstname = String()
#    gender = String()
#    lastname = String()
#    state = String()


def elastic_bank():
    s = Search(using=models.client, index="bank") \
        .query("match_all")
    response = s.execute()
    for hit in response:
        print(hit.meta.score, hit.lastname)

#    models.AccountMappingDoc.init(index="bank", using=models.client)
#    account1 = models.AccountMappingDoc.get(id=1, using=models.client, index="bank")
#    s = models.AccountMappingDoc.search()
#    s = s.query("match", gender="F")
#    s = s.query("match_all")
#    accounts = s.execute()
#    for account in accounts:
#        print(account.meta.score, account.lastname)
#    pass




def elastic_review():
    s = models.PerfumeDoc.search().index("review").using(models.client).extra(track_scores=True)
    s = s.query("match_all")
    reviews = s.execute()
    for review in reviews:
        print(review.meta.score, review.perfume)

def sharepoint_bi():
    headers = {'accept': 'application/json;odata=verbose'}
    user = 'GLOBAL\\abc1234'
    pswrd = 'xxxxxx'
    url =  'https://teamsites.iff.com/corporate/it/AppDev/BI/_api/web/'

#    r = requests.get("https://teamsites.iff.com/_api/web/title", auth=HTTPBasicAuth(user, pswrd), headers=headers)
#    r = requests.get("https://teamsites.iff.com/_api/web/title", auth=HttpNtlmAuth(user, pswrd), headers=headers)
#    r = requests.get("https://teamsites.iff.com/_api/web/lists", auth=HttpNtlmAuth(user, pswrd), headers=headers)
#    r = requests.get(url + "lists/getByTitle('SalesRepComments')/fields", auth=HttpNtlmAuth(user, pswrd), headers=headers)
    r = requests.get(url + "lists/getByTitle('SalesRepComments')/items", auth=HttpNtlmAuth(user, pswrd), headers=headers)
    select = "$select=SalesRep,Comment"
    filter = "$filter=Cycle eq '009.2012'"
    r = requests.get(url + "lists/getByTitle('SalesRepComments')/items?" + select + filter, auth=HttpNtlmAuth(user, pswrd), headers=headers)
    j = r.json()

    print( r.status_code)
    print (r.content)


def sharepoint_mi():
    headers = {'accept': 'application/json;odata=verbose'}
    user = 'GLOBAL\\abc1234'
    pswrd = 'xxxxxx'
    url =  'https://iffconnect.iff.com/Fragrances/marketintelligence/_api/web/'

#    r = requests.get(url + "title", auth=HttpNtlmAuth(user, pswrd), headers=headers)
#    r = requests.get(url + "lists?$select=EntityTypeName", auth=HttpNtlmAuth(user, pswrd), headers=headers)
#    r = requests.get(url + "lists/getByTitle('Posts')/fields", auth=HttpNtlmAuth(user, pswrd), headers=headers)
    r = requests.get(url + "lists/getByTitle('Posts')/items", auth=HttpNtlmAuth(user, pswrd), headers=headers)
#    select = "$select=SalesRep,Comment"
#    filter = "$filter=Cycle eq '009.2012'"
#    r = requests.get(url + "lists/getByTitle('Posts')/items?" + select + filter, auth=HttpNtlmAuth(user, pswrd), headers=headers)
    j = r.json()

    print( r.status_code)
    print (r.content)

def elastic_api(index, query_string, filters, aggregates):
    post_header = index + "/_search"
    post_match = {"query" : {"match" : {"_all" : q }}}
    post_filters = {}
    for filter_field, filter_value in filters.iteritems():
        post_filter = {"query" : {"term" : {filter_field : filter_value}}}
        j = j + json.dumps(post_filter)
    for aggr_field, agg_type in aggregates.iteritems():
        aggr = {"aggs" : {aggr_field : {aggr_type : {"field" : aggr_field}}}}
        j = j + json.dumps(aggr)


def elastic_py():
    #response = client.search(
    #    index="my-index",
    #    body={
    #      "query": {
    #        "filtered": {
    #          "query": {
    #            "bool": {
    #              "must": [{"match": {"title": "python"}}],
    #              "must_not": [{"match": {"description": "beta"}}]
    #            }
    #          },
    #          "filter": {"term": {"category": "search"}}
    #        }
    #      },
    #      "aggs" : {
    #        "per_tag": {
    #          "terms": {"field": "tags"},
    #          "aggs": {
    #            "max_lines": {"max": {"field": "lines"}}
    #          }
    #        }
    #      }
    #    }
    #)
    client = Elasticsearch()
    response = client.search(
        index="bank",
        body={
            "query": { "match_all": {} },
            "sort": [ { "account_number": "asc" } ]
        }
    )

    for hit in response['hits']['hits']:
        print(hit['_score'], hit['_source']['lastname'])


# Same query but using DSL


def elastic_dsl():
    #s = Search(using=client, index="my-index") \
    #    .filter("term", category="search") \
    #    .query("match", title="python")   \
    #    .query(~Q("match", description="beta"))

    #s.aggs.bucket('per_tag', 'terms', field='tags') \
    #    .metric('max_lines', 'max', field='lines')

    s = Search(using=models.client, index="bank") \
        .query("match_all")

    response = s.execute()

    for hit in response:
        print(hit.meta.score, hit.lastname)

    s = Search(using=models.client, index="bank") \
        .filter("term", state="TX") \
        .query("match_all")

    response = s.execute()

    for hit in response:
        print(hit.meta.score, hit.lastname)


# Define a default Elasticsearch client
#connections.create_connection(hosts=['localhost'])

#class Article(DocType):
#    title = String(analyzer='snowball', fields={'raw': String(index='not_analyzed')})
#    body = String(analyzer='snowball')
#    tags = String(index='not_analyzed')
#    published_from = Date()
#    lines = Integer()

#    class Meta:
#        index = 'blog'

#    def save(self, ** kwargs):
#        self.lines = len(self.body.split())
#        return super(Article, self).save(** kwargs)

#    def is_published(self):
#        return datetime.now() < self.published_from

## create the mappings in elasticsearch
#Article.init()

## create and save and article
#article = Article(meta={'id': 42}, title='Hello world!', tags=['test'])
#article.body = ''' looong text '''
#article.published_from = datetime.now()
#article.save()

#article = Article.get(id=42)
#print(article.is_published())

## Display cluster health
#print(connections.get_connection().cluster.health())


def convert_for_bulk(objmap, action=None):
    data = objmap.es_repr()
    if action == 'create':
        metadata = {
            '_op_type': action,
            "_index": objmap._meta.es_index_name,
            "_type": objmap._meta.es_type_name,
        }
        data.update(**metadata)
        bulk_data = data
    elif action == 'update':
        id = data['_id']
        data.pop('_id', None)
        bulkdata = {
            '_op_type': action,
            "_index": objmap._meta.es_index_name,
            "_type": objmap._meta.es_type_name,
            '_id': id,
            "doc_as_upsert" : 'true',
            "doc" : data
        }
    return bulkdata
