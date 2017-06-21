"""
Definition of urls for insights_crawl.
"""

from datetime import datetime
from django.conf.urls import patterns, include, url
from django.contrib import admin
from django.contrib.auth import views as auth_views
from app.forms import BootstrapAuthenticationForm
import app.views as views

# Uncomment the next lines to enable the admin:
from django.conf.urls import include
from django.contrib import admin
admin.autodiscover()

from django.contrib import auth

#params={'plugin': ModelSearchPlugin(MyModel), 'base_template': 'search.html'}
params={'plugin': '', 'base_template': 'search.html'}

import app.models as models
import app.views

urlpatterns = patterns('',
    # Examples:
    url(r'^$', app.views.home, name='home'),

    url(r'^crawl', app.views.crawl_view, name='crawl'),
    url(r'^scrape', app.views.scrape_view, name='scrape'),
    url(r'^product_elastic$', app.product.ProductElasticView.as_view(), name='product_elastic'),
    url(r'^facts$', app.views.facts_view, name='facts'),
    url(r'^fmi_admin', app.views.fmi_admin_view, name='fmi_admin'),

    url(r'^api/scrape_pollresults$', 'app.scrape_ds.scrape_pollresults_api', name='api/scrape_pollresults'),
    url(r'^api/scrape_accords$', 'app.api.scrape_accords_api', name='scrape_accords_api'),
    url(r'^api/scrape_notes$', 'app.api.scrape_notes_api', name='scrape_notes_api'),
    url(r'^api/scrape_votes$', 'app.api.scrape_votes_api', name='scrape_votes_api'),
    url(r'^api/scrape_reviews$', 'app.api.scrape_reviews_api', name='scrape_reviews_api'),

    url(r'^contact$', app.views.contact, name='contact'),
    url(r'^about', app.views.about, name='about'),

    # Registration URLs
    url(r'^accounts/register/$', views.register, name='register'),
    url(r'^accounts/register_complete/$', views.registrer_complete, name='register_complete'),
    url(r'^login/$', auth_views.login, name='login'),
    url(r'^logout/$', auth_views.logout, name='logout'),
    url(r'^admin/', admin.site.urls),

    # Uncomment the admin/doc line below to enable admin documentation:
    #url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

)