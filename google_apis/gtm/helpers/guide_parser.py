import codecs
import yaml
import re
import pprint
from apiclient.errors import HttpError
from bs4 import BeautifulSoup
from bs4 import Comment

def clean_comments(iterable):
    cleaned_text = ''
    for lines in iterable.splitlines():
        head, sep, tail = lines.partition(' //')
        lines = head
        cleaned_text += head
    return cleaned_text

def parse_implementation_guide_classic(path, virtual_pageviews=[]):

    calls_data = {'raw': []}
    comments_mapping = []
    calls_list = []

    with codecs.open(path, 'r') as f:
        html = f.read()

    comms = BeautifulSoup(html, 'lxml').find_all(string=lambda html: isinstance(html, Comment))

    for com in comms:
        catchcom = re.search('.*?:.*?\[.*?\]', com)

        if catchcom is not None:
            comments_mapping.append(eval('{' + catchcom.group() + '}'))

    soup = BeautifulSoup(html, 'lxml').find_all('code', {'class': 'language-javascript'})

    for snippets in soup:
        catchpush = re.search('dataLayer\.push\({.*?}\)', snippets.text, re.DOTALL)
        if catchpush is not None:
            catchpush = catchpush.group().replace('dataLayer.push(', '').replace('});', '}').replace('})', '}')
            catchpush = eval(clean_comments(catchpush))
            if ('event' in catchpush.keys() and catchpush['event'] not in virtual_pageviews) or 'ecommerce' in catchpush.keys():
                calls_data['raw'].append(catchpush)

    calls_data['events_mapping'] = comments_mapping

    return calls_data