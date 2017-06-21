"""
Definition of views.
"""

from django.shortcuts import render, redirect, render_to_response
from django.template.context_processors import csrf
from django.http import HttpRequest
from django.template import RequestContext
from django.views.generic.base import TemplateView
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
import json
from datetime import datetime, time
import insights_crawl.settings
import app.elastic as elastic
import app.scrape_ds as scrape_ds
import app.sentiment as sentiment
import app.product as product
import app.market as market
import app.crawl as crawl
import app.survey as survey
import app.facts as facts
import app.fmi_admin as fmi_admin
import app.azure as azure
import app.models as models
import app.survey
from .forms import *

def home(request):
    """Renders the home page."""
    assert isinstance(request, HttpRequest)
    return render(
        request,
        'app/index.html',
        context_instance = RequestContext(request,
        {
            'title':'Home Page',
            'year':datetime.now().year,
        })
    )


def scrape_view(request):
    """Renders the scrape page."""
    if request.method == 'POST':
        form = scrape_form(request.POST)
        if form.is_valid():
            site_choices = form.cleaned_data['site_choices_field']
            scrape_choices = form.cleaned_data['scrape_choices_field']
            brand_field = form.cleaned_data['brand_field']
            if 'scrape' in form.data:
                models.scrape_li = scrape_ds.scrape_ds(site_choices, scrape_choices, brand_field)
                if not product.scrape_save(brand_field):
                    form.add_form_error("Could not save scrape results")
            if 'retrieve' in form.data:
                if not product.scrape_retrieve(brand_field):
                    form.add_form_error("Could not retrieve scrape results")
            if len(models.scrape_li) == 0:
                form.add_form_error("First retrieve or scrape the web for this brand")
            else:
                if 'explore' in form.data:
                    return render(request, 'app/scraperesults.html', {'brand': brand_field, 'scrape_li' : models.scrape_li } )
                if 'sentiment' in form.data:
                    sentiment.sentiment(brand_field)
                    if not product.scrape_save(brand_field):
                        form.add_form_error("Could not save scrape results")
                    return render(request, 'app/scraperesults.html', {'brand': brand_field, 'scrape_li' : models.scrape_li } )
            return render(request, 'app/scrape.html', {'form': form, 'scrape_li' : models.scrape_li } )
    else:
        form = scrape_form(initial={'site_choices_field':['fragrantica'],'scrape_choices_field':['accords','moods','notes']})

    return render(request, 'app/scrape.html', {'form': form },
                  context_instance = RequestContext(request, {'message':'IFF - Insight Platform', 'year':datetime.now().year,} ))


def crawl_view(request):
    """Renders the crawl page."""
    if request.method == 'POST':
        form = crawl_form(request.POST)
        form.is_valid()
        ci_filename = form.cleaned_data['ci_filename_field']
        # called form crawlresults.html
        if 'crawl_survey' in form.data:
            crawl.crawl_survey(ci_filename)
        # called from crawh.html
        if form.is_valid():
            #index_choices = form.cleaned_data['index_choices_field']
            from_date = form.cleaned_data['from_date']
            nrpages = form.cleaned_data['nrpages_field']
            site_choices = form.cleaned_data['site_choices_field']
            scrape_choices = form.cleaned_data['scrape_choices_field']
            rss_field = form.cleaned_data['rss_field']
            product_field = form.cleaned_data['product_field']
            username = form.cleaned_data['username']
            password = form.cleaned_data['password']
            cft_filename = form.cleaned_data['cft_filename_field']
            ci_filename = form.cleaned_data['ci_filename_field']
            if from_date == None:
                today = datetime.now()
                from_date = datetime(today.year-1, 1, 1, 0, 0, 0)
            if 'crawl_si_sites' in form.data:
                for site_choice in site_choices:
                    if site_choice == 'apf':
                        crawl.crawl_apf(scrape_choices, nrpages)
                    elif site_choice == 'cosmetics':
                        crawl.crawl_cosmetic(scrape_choices, nrpages)
                    else:
                        crawl.si_site(site_choice, nrpages)
            elif 'crawl_mi' in form.data:
                if not market.index_posts(from_date, username, password):
                    form.add_form_error("Could not index category posts")
            if 'crawl_pi' in form.data:
                if product_field == '':
                    form.add_form_error("Specify a product")
                else:
                    if not product.crawl_product(index_choices, product_field):
                        form.add_form_error("Could not save product data")
            if 'index_pi' in form.data:
                if product_field == '':
                    form.add_form_error("Specify a product")
                else:
                    if not product.index_product(index_choices, product_field):
                        form.add_form_error("Could not retrieve product data")
            if 'crawl_feedly' in form.data:
                if crawl.crawl_feedly(from_date, rss_field) == False:
                    form.add_form_error("Feedly crawl failed (authorization expired")
            if 'crawl_scentemotion' in form.data:
                crawl.crawl_scentemotion(cft_filename)
            if 'map_survey' in form.data:
                col_map = crawl.map_survey(ci_filename)
                answers = list(survey.col2ans.keys())
                answers = sorted(answers)
                context = {
                    'form'      : form,
                    'col_map'   : col_map,
                    'col2ans'   : survey.col2ans,
                    'answers'   : answers,
                    'ansmap'    : survey.ansmap,
                    'ans2qst'   : survey.ans2qst,
                    'dashboard' : []
                    }
                return render(request, 'app/crawlresults.html', context )
            if 'return_survey' in form.data:
                pass
            return render(request, 'app/crawl.html', {'form': form, 'es_hosts' : insights_crawl.settings.ES_HOSTS, 'scrape_li' : models.scrape_li } )
    else:
        form = crawl_form(initial={'scrape_choices_field':['product', 'blog']})

    return render(request, 'app/crawl.html', {'form': form, 'es_hosts' : insights_crawl.settings.ES_HOSTS },
                  context_instance = RequestContext(request, {'message':'IFF - Insight Platform', 'year':datetime.now().year,} ))

def facts_view(request):
    """Renders the facts page."""
    if request.method == 'POST':
        form = facts_form(request.POST)
        if form.is_valid():
            facts_choices = form.cleaned_data['facts_choices_field']
            norms_choices = form.cleaned_data['norms_choices_field']
            survey_field = form.cleaned_data['survey_field']
            facts_d = facts.facts_survey(survey_field, facts_choices, norms_choices)
            crawl.crawl_studies_facts(survey_field, facts_d)
            return render(request, 'app/factsresults.html', {'facts_d' : facts_d } )
    else:
        form = facts_form(initial={'facts_choices_field':['emotions'],'norms_choices_field':['age']})

    return render(request, 'app/facts.html', {'form': form },
                  context_instance = RequestContext(request, {'message':'IFF - Insight Platform', 'year':datetime.now().year,} ))


def fmi_admin_view(request):
    """Renders the Admin Index page."""
    if request.method == 'POST':
        form = fmi_admin_form(request.POST)
        if form.is_valid():
            index_choices = form.cleaned_data['index_choices_field']
            opml_filename = form.cleaned_data['opml_filename_field']
            keyword_filename = form.cleaned_data['keyword_filename_field']
            if 'index_elastic' in form.data:
                fmi_admin.create_index_elastic(index_choices)
            elif 'analyzer' in form.data:
                fmi_admin.create_analyzer(index_choices)
            if 'index_azure' in form.data:
                azure.create_index_azure(index_choices)
            elif 'export_opml' in form.data:
                if not fmi_admin.export_opml(index_choices, opml_filename):
                    form.add_form_error("Could not export OPML")
            elif 'import_opml' in form.data:
                if not fmi_admin.import_opml(index_choices, opml_filename):
                    form.add_form_error("Could not import OPML")
            elif 'keywords' in form.data:
                if not fmi_admin.read_keywords(index_choices, keyword_filename):
                    form.add_form_error("Could not read keywords file")
            return render(request, 'app/fmi_admin.html', {'form': form })
    else:
        form = fmi_admin_form(initial={'index_choices_field':['cosmetic']})

    return render(request, 'app/fmi_admin.html', {'form': form },
                  context_instance = RequestContext(request, {'message':'IFF - Insight Platform', 'year':datetime.now().year,} ))


def contact(request):
    """Renders the contact page."""
    assert isinstance(request, HttpRequest)
    return render(
        request,
        'app/contact.html',
        context_instance = RequestContext(request,
        {
            'title':'Contact',
            'message':'Your contact page.',
            'year':datetime.now().year,
        })
    )

def about(request):
    """Renders the about page."""
    assert isinstance(request, HttpRequest)
    return render(
        request,
        'app/about.html',
        context_instance = RequestContext(request,
        {
            'title':'About',
            'message':'Your application description page.',
            'year':datetime.now().year,
        })
    )

def register(request):
    if request.method == 'POST':
        form = RegistrationForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('/accounts/register_complete')

    else:
        form = RegistrationForm()
    token = {}
    token.update(csrf(request))
    token['form'] = form

    return render_to_response('registration/register.html', token)

def registrer_complete(request):
    return render_to_response('registration/registrer_complete.html')