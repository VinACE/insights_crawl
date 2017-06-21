"""
Definition of api-views.
"""

from pandas import Series, DataFrame
from django.shortcuts import render
from django.http import HttpRequest
from django.http import HttpResponse
from django.http import HttpResponseRedirect
#from django.http import JsonResponse
from django.template import RequestContext
from django.core.files import File
from datetime import datetime
import json
import app.models as models

from .scrape_ds import *


def scrape_accords_api(request):
    accords_df_json = scrape_accords_json()
    return HttpResponse(accords_df_json, content_type='application/json')

def scrape_notes_api(request):
    notes_df_json = scrape_notes_json()
    return HttpResponse(notes_df_json, content_type='application/json')

def scrape_votes_api(request):
    votes_df_json = scrape_votes_json()
    return HttpResponse(votes_df_json, content_type='application/json')

def scrape_reviews_api(request):
    reviews_df_json = scrape_reviews_json()
    return HttpResponse(reviews_df_json, content_type='application/json')




