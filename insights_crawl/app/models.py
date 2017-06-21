"""
Definition of models.
"""

from django.conf import settings
from django.db import models
from django.utils import timezone
from django.views.generic.base import TemplateView

# Create your models here.
import queue
import datetime
import insights_crawl.settings
from pandas import DataFrame

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl import DocType, Date, Double, Long, Integer, Boolean
from elasticsearch_dsl.connections import connections

from django.utils.encoding import python_2_unicode_compatible

client = Elasticsearch(insights_crawl.settings.ES_HOSTS)


import django.db.models.options as options
options.DEFAULT_NAMES = options.DEFAULT_NAMES + (
    'es_index_name', 'es_type_name', 'es_mapping'
)


###
### Fragrantica
###

# Class name has to match the name of the mapping in ES (doc_type)
class Perfume(models.Model):
    perfume = models.CharField(max_length=200)
    review_date = models.DateField()
    review = models.TextField()
    label = models.CharField(max_length=10)
    accords = models.TextField()
    img_src = models.TextField()

class Review(models.Model):
    reviewid = models.IntegerField()
    perfume = models.CharField(max_length=200)
    review_date = models.DateField()
    review = models.TextField()
    label = models.CharField(max_length=10)
    accords = []
    img_src = models.TextField()

    class Meta:
        es_index_name = 'review'
        es_type_name = 'perfume'
        es_mapping = {
            'properties' : {
                'perfume'       : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                'review_date'   : {'type' : 'date'},
                'review'        : {'type' : 'text'},
                'label'         : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                'accords'       : {
                    'properties' : {
                        'accord' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'votes'  : {'type' : 'integer'},
                        }
                    },
                'img_src'        : {'type' : 'text'},
                }
            }

    def es_repr(self):
        data = {}
        mapping = self._meta.es_mapping
        data['_id'] = self.reviewid
        for field_name in mapping['properties'].keys():
            data[field_name] = self.field_es_repr(field_name)
        return data
    def field_es_repr(self, field_name):
        config = self._meta.es_mapping['properties'][field_name]
        if hasattr(self, 'get_es_%s' % field_name):
            field_es_value = getattr(self, 'get_es_%s' % field_name)()
        else:
            if config['type'] == 'object':
                related_object = getattr(self, field_name)
                field_es_value = {}
                field_es_value['_id'] = related_object.pk
                for prop in config['properties'].keys():
                    field_es_value[prop] = getattr(related_object, prop)
            else:
                field_es_value = getattr(self, field_name)
        return field_es_value
    def get_es_accords(self):
        return [{'accord': accord, 'votes': votes} for accord, votes in self.accords.items()]



###
### Market Intelligence
###

class Post(models.Model):
    editor_id = models.CharField(max_length=30)
    published_date = models.DateField()
    post_category_id = models.CharField(max_length=30)
    title = models.CharField(max_length=256)
    relevance = models.TextField()
    topline = models.TextField()
    source = models.TextField()
    article = models.TextField()
    average_rating = models.FloatField()
    rating_count = models.IntegerField()
    num_comments_id = models.IntegerField()

class PostMap(models.Model):
    post_id = models.IntegerField()
    editor_id = models.CharField(max_length=30)
    published_date = models.DateField()
    post_category_id = models.CharField(max_length=30)
    title = models.CharField(max_length=256)
    relevance = models.TextField()
    topline = models.TextField()
    source = models.TextField()
    article = models.TextField()
    average_rating = models.FloatField()
    rating_count = models.IntegerField()
    num_comments_id = models.IntegerField()
    class Meta:
        es_index_name = 'post'
        es_type_name = 'post'
        es_mapping = {
            'properties' : {
                'editor_id'         : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                'published_date'    : {'type' : 'date'},
                'post_category_id'  : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                'title'             : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                'relevance'         : {'type' : 'text'},
                'topline'           : {'type' : 'text'},
                'source'            : {'type' : 'text'},
                #'article'           : {'type' : 'text', "fields" : { "raw": { "type":  "keyword" }}},
                'article'           : {'type' : 'text'},
                'average_rating'    : {'type' : 'float'},
                'rating_count'      : {'type' : 'integer', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                'num_comments_id'   : {'type' : 'integer', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                }
            }
    def es_repr(self):
        data = {}
        mapping = self._meta.es_mapping
        data['_id'] = self.post_id
        for field_name in mapping['properties'].keys():
            data[field_name] = self.field_es_repr(field_name)
        return data
    def field_es_repr(self, field_name):
        config = self._meta.es_mapping['properties'][field_name]
        if hasattr(self, 'get_es_%s' % field_name):
            field_es_value = getattr(self, 'get_es_%s' % field_name)()
        else:
            if config['type'] == 'object':
                related_object = getattr(self, field_name)
                field_es_value = {}
                field_es_value['_id'] = related_object.pk
                for prop in config['properties'].keys():
                    field_es_value[prop] = getattr(related_object, prop)
            else:
                field_es_value = getattr(self, field_name)
        return field_es_value


###
### Costmetics
###

class Page(models.Model):
    page_id = models.IntegerField()
    posted_date = models.DateField()
    site = models.TextField()
    sub_site = models.TextField()
    section = models.TextField()
    title = models.TextField()
    url = models.TextField()
    page = models.TextField()

class PageMap(models.Model):
    page_id = models.IntegerField()
    posted_date = models.DateField()
    site = models.TextField()
    sub_site = models.TextField()
    section = models.TextField()
    title = models.TextField()
    url = models.TextField()
    page = models.TextField()

    class Meta:
        es_index_name = 'page'
        es_type_name = 'page'
        es_mapping = {
            'properties' : {
                'posted_date'   : {'type' : 'date'},
                'site'          : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                'sub_site'      : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                'section'       : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                'title'         : {'type' : 'text'},
                'url'           : {'type' : 'text'},
                'page'          : {'type' : 'text'},
                }
            }
    def es_repr(self):
        data = {}
        mapping = self._meta.es_mapping
        data['_id'] = self.page_id
        for field_name in mapping['properties'].keys():
            data[field_name] = self.field_es_repr(field_name)
        return data
    def field_es_repr(self, field_name):
        config = self._meta.es_mapping['properties'][field_name]
        if hasattr(self, 'get_es_%s' % field_name):
            field_es_value = getattr(self, 'get_es_%s' % field_name)()
        else:
            if config['type'] == 'object':
                related_object = getattr(self, field_name)
                field_es_value = {}
                field_es_value['_id'] = related_object.pk
                for prop in config['properties'].keys():
                    field_es_value[prop] = getattr(related_object, prop)
            else:
                field_es_value = getattr(self, field_name)
        return field_es_value

###
### FEEDLY
###

class Feedly(models.Model):
    post_id = models.IntegerField()
    published_date = models.DateField()
    category = models.TextField()
    feed = models.TextField()
    feed_topics = models.TextField()
    body_topics = models.TextField()
    title = models.TextField()
    url = models.TextField()
    body = models.TextField()
 
class FeedlyMap(models.Model):
    post_id = models.IntegerField()
    published_date = models.DateField()
    category = models.TextField()
    feed = models.TextField()
    feed_topics = models.TextField()
    body_topics = models.TextField()
    title = models.TextField()
    url = models.TextField()
    body = models.TextField()

    class Meta:
        es_index_name = 'feedly'
        es_type_name = 'feedly'
        es_mapping = {
            'properties' : {
                'published_date'    : {'type' : 'date'},
                'category'          : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                'feed'              : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                'feed_topics'       : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                'body_topics'       : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                'title'             : {'type' : 'text'},
                'url'               : {'type' : 'text'},
                'body'              : {'type' : 'text'},
                #'body'              : {'type' : 'text', 'fields' : {
                #    'body_keepwords'    : {'type': 'text', 'analyzer': 'keepwords'},
                #    'body_keeplength'   : {'type': 'token_count', 'analyzer': 'keepwords'}}},
                }
            }
    def es_repr(self):
        data = {}
        mapping = self._meta.es_mapping
        data['_id'] = self.post_id
        for field_name in mapping['properties'].keys():
            data[field_name] = self.field_es_repr(field_name)
        return data
    def field_es_repr(self, field_name):
        config = self._meta.es_mapping['properties'][field_name]
        if hasattr(self, 'get_es_%s' % field_name):
            field_es_value = getattr(self, 'get_es_%s' % field_name)()
        else:
            if config['type'] == 'object':
                related_object = getattr(self, field_name)
                field_es_value = {}
                field_es_value['_id'] = related_object.pk
                for prop in config['properties'].keys():
                    field_es_value[prop] = getattr(related_object, prop)
            else:
                field_es_value = getattr(self, field_name)
        return field_es_value


###
### Scent Emotion (CFT - Ingredients)
###

class Scentemotion(models.Model):
    cft_id = models.IntegerField()
    dataset = models.TextField()
    ingr_name = models.TextField()
    IPC = models.TextField()
    supplier = models.TextField()
    olfactive = models.TextField()
    region = models.TextField()
    review = models.TextField()
    dilution = models.TextField()
    intensity = models.TextField()
    mood = models.TextField()
    smell = models.TextField()
    negative = models.TextField()
    descriptor = models.TextField()
    color = models.TextField()
    texture = models.TextField()
    emotion = models.TextField()
    hedonics = models.TextField()

 
class ScentemotionMap(models.Model):
    cft_id = models.IntegerField()
    dataset = models.TextField()
    ingr_name = models.TextField()
    IPC = models.TextField()
    supplier = models.TextField()
    olfactive = models.TextField()
    region = models.TextField()
    review = models.TextField()
    dilution = models.TextField()
    intensity = models.TextField()
    mood = []
    smell = []
    negative = []
    descriptor = []
    color = []
    texture = []
    emotion = []
    hedonics = []

    class Meta:
        es_index_name = 'scentemotion'
        es_type_name = 'scentemotion'
        es_mapping = {
            "properties" : {
                "dataset"           : {"type" : "string", "fields" : {"keyword" : {"type" : "keyword", "ignore_above" : 256}}},
                "ingr_name"         : {"type" : "string", "fields" : {"raw" : {"type" : "string", "index" : "not_analyzed"}}},
                "IPC"               : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "supplier"          : {"type" : "string", "fields" : {"keyword" : {"type" : "keyword", "ignore_above" : 256}}},
                "olfactive"         : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "region"            : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "review"            : {"type" : "text"},
                "dilution"          : {"type" : "string", "fields" : {"raw" : {"type" : "string", "index" : "not_analyzed"}}},
                "intensity"         : {"type" : "string", "fields" : {"raw" : {"type" : "string", "index" : "not_analyzed"}}},
                'mood'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'smell'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'negative'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'descriptor'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'color'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'texture'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'emotion'              : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'hedonics'              : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                }
            }

    def es_repr(self):
        data = {}
        mapping = self._meta.es_mapping
        data['_id'] = self.cft_id
        for field_name in mapping['properties'].keys():
            data[field_name] = self.field_es_repr(field_name)
        return data
    def get_es_mood(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_smell(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_negative(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_descriptor(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_color(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_texture(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_emotion(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_hedonics(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def field_es_repr(self, field_name):
        config = self._meta.es_mapping['properties'][field_name]
        if hasattr(self, 'get_es_%s' % field_name):
            field_es_value = getattr(self, 'get_es_%s' % field_name)(field_name)
        else:
            if config['type'] == 'object':
                related_object = getattr(self, field_name)
                field_es_value = {}
                field_es_value['_id'] = related_object.pk
                for prop in config['properties'].keys():
                    field_es_value[prop] = getattr(related_object, prop)
            else:
                field_es_value = getattr(self, field_name)
                if config['type'] == 'integer' and type(field_es_value) == str:
                    field_es_value = int(float(field_es_value))
                if (config['type'] == 'string' or config['type'] == 'text') and type(field_es_value) == int:
                    field_es_value = "{0:d}".format(field_es_value)
        return field_es_value



###
### Scent Emotion (CFT - CI Studies)
###

class Studies(models.Model):
    cft_id = models.IntegerField()
    dataset = models.TextField()
    ingr_name = models.TextField()
    IPC = models.TextField()
    olfactive = models.TextField()
    region = models.TextField()
    intensity = models.TextField()
    perception = models.TextField()
    method = models.TextField()
    product_form = models.TextField()
    freshness = models.IntegerField()
    cleanliness = models.IntegerField()
    lastingness  = models.IntegerField()
    intensity = models.IntegerField()
    liking = models.TextField()
    concept = models.TextField()
    emotion = models.TextField()
    fragrattr = models.TextField()
    mood = models.TextField()
    smell = models.TextField()
    suitable_product = models.TextField()
    suitable_stage = models.TextField()
    hedonics = models.TextField()

 
class StudiesMap(models.Model):
    cft_id = models.IntegerField()
    dataset = models.TextField()
    ingr_name = models.TextField()
    IPC = models.TextField()
    olfactive = models.TextField()
    region = models.TextField()
    intensity = models.TextField()
    perception = []
    method = []
    product_form = []
    freshness = []
    cleanliness = []
    lastingness  = []
    intensity = []
    liking = []
    concept = []
    emotion = []
    fragrattr = []
    mood = []
    smell = []
    suitable_product = []
    suitable_stage = []
    hedonics = []

    class Meta:
        es_index_name = 'studies'
        es_type_name = 'studies'
        es_mapping = {
            "properties" : {
                "dataset"           : {"type" : "string", "fields" : {"keyword" : {"type" : "keyword", "ignore_above" : 256}}},
                "ingr_name"         : {"type" : "string", "fields" : {"raw" : {"type" : "string", "index" : "not_analyzed"}}},
                "IPC"               : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "olfactive"         : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "region"            : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "intensity"         : {"type" : "string", "fields" : {"raw" : {"type" : "string", "index" : "not_analyzed"}}},
                'perception'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'method'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'product_form'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'freshness'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'cleanliness'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'lastingness'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'intensity'              : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'liking'              : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },

                'concept'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'emotion'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'fragrattr'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'mood'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'smell'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'suitable_product'         : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'suitable_stage'              : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                'hedonics'              : {
                    'type'       : 'nested',
                    'properties' : {
                        'val' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'prc' : {'type' : 'float'},
                        }
                    },
                }
            }

    def es_repr(self):
        data = {}
        mapping = self._meta.es_mapping
        data['_id'] = self.cft_id
        for field_name in mapping['properties'].keys():
            data[field_name] = self.field_es_repr(field_name)
        return data
    def get_es_perception(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_method(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_product_form(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_freshness(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_cleanliness(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_lastingness(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_intensity(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_liking(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_concept(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_emotion(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_fragrattr(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_mood(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_smell(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_suitable_product(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_suitable_stage(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def get_es_hedonics(self, field_name):
        list_es_value = getattr(self, field_name)
        field_es_value = [{'val':t[0], 'prc':t[1]} for t in list_es_value]
        return field_es_value
    def field_es_repr(self, field_name):
        config = self._meta.es_mapping['properties'][field_name]
        if hasattr(self, 'get_es_%s' % field_name):
            field_es_value = getattr(self, 'get_es_%s' % field_name)(field_name)
        else:
            if config['type'] == 'object':
                related_object = getattr(self, field_name)
                field_es_value = {}
                field_es_value['_id'] = related_object.pk
                for prop in config['properties'].keys():
                    field_es_value[prop] = getattr(related_object, prop)
            else:
                field_es_value = getattr(self, field_name)
                if config['type'] == 'integer' and type(field_es_value) == str:
                    field_es_value = int(float(field_es_value))
                if (config['type'] == 'string' or config['type'] == 'text') and type(field_es_value) == int:
                    field_es_value = "{0:d}".format(field_es_value)
        return field_es_value

###
### Survey (CI)
###

class Survey(models.Model):
    resp_id = models.TextField()
    survey = models.TextField()
    country = models.TextField()
    cluster = models.TextField()
    gender = models.TextField()
    age = models.TextField()
    children = models.TextField()
    ethnics = models.TextField()
    city = models.TextField()
    regions = models.TextField()
    education = models.TextField()
    income = models.TextField()
    blindcode = models.TextField()
    brand = models.TextField()
    variant = models.TextField()
    olfactive = models.TextField()
    perception = models.TextField()
    method = models.TextField()
    product_form = models.TextField()
    freshness = models.IntegerField()
    cleanliness = models.IntegerField()
    lastingness  = models.IntegerField()
    intensity = models.IntegerField()
    liking = models.TextField()
    concept = models.TextField()
    emotion = models.TextField()
    fragrattr = models.TextField()
    mood = models.TextField()
    smell = models.TextField()
    suitable_product = models.TextField()
    suitable_stage = models.TextField()
    question = models.TextField()
 
class SurveyMap(models.Model):
    resp_id = models.TextField()
    survey = models.TextField()
    country = models.TextField()
    cluster = models.TextField()
    gender = models.TextField()
    age = models.TextField()
    children = models.TextField()
    ethnics = models.TextField()
    city = models.TextField()
    regions = models.TextField()
    education = models.TextField()
    income = models.TextField()
    blindcode = models.TextField()
    brand = models.TextField()
    variant = models.TextField()
    olfactive = models.TextField()
    perception = models.TextField()
    method = models.TextField()
    product_form = models.TextField()
    freshness = models.IntegerField()
    cleanliness = models.IntegerField()
    lastingness  = models.IntegerField()
    intensity = models.IntegerField()
    liking = models.TextField()
    concept = []
    emotion = []
    fragrattr = []
    mood = []
    smell = []
    suitable_product = []
    suitable_stage = []
    question = []

    class Meta:
        es_index_name = 'survey'
        es_type_name = 'survey'
        es_mapping = {
            "properties" : {
                "survey"            : {"type" : "string", "fields" : {"keyword" : {"type" : "keyword", "ignore_above" : 256}}},
                "country"           : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "cluster"           : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "gender"            : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "age"               : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                'children'          : {
                    'type'       : 'nested',
                    'properties' : {
                        'question' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'answer'   : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        }
                    },
                "ethnics"           : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "city"              : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "regions"           : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "education"         : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "income"            : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "blindcode"            : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "brand"             : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "variant"           : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "olfactive"         : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "perception"        : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "method"            : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                "product_form"      : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                'freshness'         : {'type' : 'integer'},
                'cleanliness'       : {'type' : 'integer'},
                'lastingness'       : {'type' : 'integer'},
                'intensity'         : {'type' : 'integer'},
                "liking"            : {"type" : "string", "fields" : {
                                            "keyword" : {"type" : "keyword", "ignore_above" : 256},
                                            "raw" : {"type" : "string", "index" : "not_analyzed"}
                                       }},
                'concept'              : {
                    'type'       : 'nested',
                    'properties' : {
                        'question' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'answer'   : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        }
                    },
                'emotion'              : {
                    'type'       : 'nested',
                    'properties' : {
                        'question' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'answer'   : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        }
                    },
                'fragrattr'          : {
                    'type'       : 'nested',
                    'properties' : {
                        'question' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'answer'   : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        }
                    },
                'mood'              : {
                    'type'       : 'nested',
                    'properties' : {
                        'question' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'answer'   : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        }
                    },
                'smell'          : {
                    'type'       : 'nested',
                    'properties' : {
                        'question' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'answer'   : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        }
                    },
                'suitable_product'       : {
                    'type'       : 'nested',
                    'properties' : {
                        'question' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'answer'   : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        }
                    },
                'suitable_stage'       : {
                    'type'       : 'nested',
                    'properties' : {
                        'question' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'answer'   : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        }
                    },
                'question'          : {
                    'type'       : 'nested',
                    'properties' : {
                        'question' : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        'answer'   : {'type' : 'string', 'fields' : {'keyword' : {'type' : 'keyword', 'ignore_above' : 256}}},
                        }
                    },
                }
            }

    def es_repr(self):
        data = {}
        mapping = self._meta.es_mapping
        data['_id'] = self.resp_id
        for field_name in mapping['properties'].keys():
            data[field_name] = self.field_es_repr(field_name)
        return data
    def field_es_repr(self, field_name):
        config = self._meta.es_mapping['properties'][field_name]
        if hasattr(self, 'get_es_%s' % field_name):
            field_es_value = getattr(self, 'get_es_%s' % field_name)(field_name)
        else:
            if config['type'] == 'object':
                related_object = getattr(self, field_name)
                field_es_value = {}
                field_es_value['_id'] = related_object.pk
                for prop in config['properties'].keys():
                    field_es_value[prop] = getattr(related_object, prop)
            else:
                field_es_value = getattr(self, field_name)
                if config['type'] == 'integer' and type(field_es_value) == str:
                    field_es_value = int(float(field_es_value))
        return field_es_value
    def get_es_children(self, field_name):
        return [{'question': q, 'answer': a} for q, a in self.children.items()]
    def get_es_concept(self, field_name):
        return [{'question': q, 'answer': a} for q, a in self.concept.items()]
    def get_es_emotion(self, field_name):
        return [{'question': q, 'answer': a} for q, a in self.emotion.items()]
    def get_es_fragrattr(self, field_name):
        return [{'question': q, 'answer': a} for q, a in self.fragrattr.items()]
    def get_es_mood(self, field_name):
        return [{'question': q, 'answer': a} for q, a in self.mood.items()]
    def get_es_smell(self, field_name):
        return [{'question': q, 'answer': a} for q, a in self.smell.items()]
    def get_es_suitable_product(self, field_name):
        return [{'question': q, 'answer': a} for q, a in self.suitable_product.items()]
    def get_es_suitable_stage(self, field_name):
        return [{'question': q, 'answer': a} for q, a in self.suitable_stage.items()]
    def get_es_question(self, field_name):
        return [{'question': q, 'answer': a} for q, a in self.question.items()]


    ### GLOBAL VARIABLES
             
scrape_li = []
posts_df = DataFrame()
search_keywords = {}

scrape_q = queue.Queue()



