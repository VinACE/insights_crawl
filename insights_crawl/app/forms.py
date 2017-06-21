"""
Definition of forms.
"""

from django import forms
from django.forms.utils import ErrorList
from django.forms.utils import ErrorDict
from django.forms.forms import NON_FIELD_ERRORS
from django.contrib.auth.forms import AuthenticationForm
from django.utils.translation import ugettext_lazy as _

from django.contrib.auth.models import User
from django.contrib.auth.forms import UserCreationForm

class BootstrapAuthenticationForm(AuthenticationForm):
    """Authentication form which uses boostrap CSS."""
    username = forms.CharField(max_length=254,
                               widget=forms.TextInput({
                                   'class': 'form-control',
                                   'placeholder': 'User name'}))
    password = forms.CharField(label=_("Password"),
                               widget=forms.PasswordInput({
                                   'class': 'form-control',
                                   'placeholder':'Password'}))

class scrape_form(forms.Form):
    site_choices = (('fragrantica', 'Fragrantica'), ('amazon', 'Amazon'), ('sephora', 'Sephora'), ('bbw', 'BBW'), ('fotw', 'Fragrances of the World'))
    site_choices_field = forms.MultipleChoiceField(label='Web Site', choices=site_choices, widget=forms.CheckboxSelectMultiple, required=True)
    scrape_choices = (('accords', 'Accords'), ('moods', 'Moods'), ('notes', 'Notes'), ('reviews', 'Reviews'), ('longevity', 'Longevity'), ('sillage', 'Sillage'))
    scrape_choices_field = forms.MultipleChoiceField(label='Scrape', choices=scrape_choices, widget=forms.CheckboxSelectMultiple, required=True)
    brand_field = forms.CharField(label='Brand', max_length=40, required = True, initial = '', help_text='Scrape for this brand')
    def add_form_error(self, message):
        if not self._errors:
            self._errors = ErrorDict()
        if not NON_FIELD_ERRORS in self._errors:
            self._errors[NON_FIELD_ERRORS] = self.error_class()
        self._errors[NON_FIELD_ERRORS].append(message)

class facts_form(forms.Form):
    survey_field = forms.CharField(label='Survey', max_length=40, required = True, initial = '', help_text='Survey for which to create Facts and Norms')
    facts_choices = (('emotion', 'Emotion'), ('concept', 'Concept'), ('suitable_product', 'Suitable Product'), ('suitable_stage', 'Suitable Stage'),
                     ('intensity', 'Intensity'), ('freshness', 'Freshness'), ('cleanliness', 'Cleanliness'), ('lastingness', 'Lastingness'),
                     ('liking.keyword', 'Linking/Hedonics'))
    facts_choices_field = forms.MultipleChoiceField(label='Facts', choices=facts_choices, widget=forms.CheckboxSelectMultiple, required=True)
    norms_choices = (('country', 'Country'), ('gender', 'Gender'), ('children', 'Children'), ('education', 'Education'), ('income', 'Income'),
                     ('age', 'Age groups'), ('ethnics', 'Ethnics'))
    norms_choices_field = forms.MultipleChoiceField(label='Norms', choices=norms_choices, widget=forms.CheckboxSelectMultiple, required=True)
    def add_form_error(self, message):
        if not self._errors:
            self._errors = ErrorDict()
        if not NON_FIELD_ERRORS in self._errors:
            self._errors[NON_FIELD_ERRORS] = self.error_class()
        self._errors[NON_FIELD_ERRORS].append(message)

class product_form(forms.Form):
    product_field = forms.CharField(label='Product', max_length=40, required = False, initial = '', help_text='Index this product')
    def add_form_error(self, message):
        if not self._errors:
            self._errors = ErrorDict()
        if not NON_FIELD_ERRORS in self._errors:
            self._errors[NON_FIELD_ERRORS] = self.error_class()
        self._errors[NON_FIELD_ERRORS].append(message)

class crawl_form(forms.Form):
    #index_choices = (('elastic', 'Elastic Index/Search'), ('azure', 'Azure Index/Search'))
    #index_choices_field = forms.MultipleChoiceField(label='Index', choices=index_choices, widget=forms.CheckboxSelectMultiple, required=True)
    from_date = forms.DateField(required=False)
    nrpages_field = forms.IntegerField(label='Number of Pages to Scrape', initial = 50)
    site_choices = (('cosmetics', 'Cosmetics'), ('apf', 'APF'), ('contagious', 'Contagious'), ('mit', 'MIT Media Lab'), ('gci', 'GCI magazine'))
    site_choices_field = forms.MultipleChoiceField(label='Web Site', choices=site_choices, widget=forms.CheckboxSelectMultiple, required=False)
    scrape_choices = (('market', 'Market'), ('business', 'Business'), ('product', 'Product'), ('events', 'Events'),
                      ('publications', 'Publications'), ('blog', 'Blog'))
    scrape_choices_field = forms.MultipleChoiceField(label='Scrape', choices=scrape_choices, widget=forms.CheckboxSelectMultiple, required=True)
    rss_field = forms.CharField(label='RSS Category', max_length=40, required = False, initial = '', help_text='Crawl this category')
    product_field = forms.CharField(label='Product', max_length=40, required = False, initial = '', help_text='Index this product')
    username = forms.CharField(max_length=254, widget=forms.TextInput({'class': 'form-control','placeholder': 'User name'}), required=False)
    password = forms.CharField(label=_("Password"), widget=forms.PasswordInput({'class': 'form-control','placeholder':'Password'}), required=False)
    cft_filename_field = forms.CharField(label='CFT file', max_length=40, required = False, initial = 'CFT Ing.csv')
    ci_filename_field = forms.CharField(label='CI file', max_length=40, required = False, initial = 'fresh and clean Survey test.csv')
    #ci_filename_field = forms.CharField(label='CI file', max_length=40, required = False, initial = 'ChoiceModel FF USA.csv')
    def add_form_error(self, message):
        if not self._errors:
            self._errors = ErrorDict()
        if not NON_FIELD_ERRORS in self._errors:
            self._errors[NON_FIELD_ERRORS] = self.error_class()
        self._errors[NON_FIELD_ERRORS].append(message)

class fmi_admin_form(forms.Form):
    index_choices = (('pi', 'Product Intelligence'), ('mi', 'MI - Market Intelligence'), ('si_sites', 'SI - Sites'),
                     ('feedly', 'Feedly'), ('scentemotion', 'Scent Emotion'), ('studies', 'CI/SE Studies'), ('survey', 'CI Survey'))
    index_choices_field = forms.MultipleChoiceField(label='Web Site', choices=index_choices, widget=forms.CheckboxSelectMultiple, required=True)
    opml_filename_field = forms.CharField(label='OPML file', max_length=40, required = False, initial = '')
    keyword_filename_field = forms.CharField(label='Keyword file', max_length=40, required = False, initial = '')
    def add_form_error(self, message):
        if not self._errors:
            self._errors = ErrorDict()
        if not NON_FIELD_ERRORS in self._errors:
            self._errors[NON_FIELD_ERRORS] = self.error_class()
        self._errors[NON_FIELD_ERRORS].append(message)


class RegistrationForm(UserCreationForm):
    email = forms.EmailField(required=True)
    
    class Meta:
        model = User
        fields = ('username', 'first_name', 'last_name', 'email', 'password1', 'password2')
        
    def save(self, commit=True):
        user = super(RegistrationForm, self).save(commit=False)
        user.first_name = self.cleaned_data['first_name']
        user.last_name = self.cleaned_data['last_name']
        user.email = self.cleaned_data['email']
        
        if commit:
            user.save()
            
        return user
