'''
Script for saving data from NYT Article Search API into csv
'''

from absl import app, flags, logging
import os
import requests
import time
import numpy as np
import pandas as pd

flags.DEFINE_string('api_key', os.environ['NYT_API_KEY'], 'api_key')
flags.DEFINE_string('begin_date', '20170301', 'only get results after this date')
flags.DEFINE_string('dest', None, 'csv file path to save data frame')
flags.DEFINE_string('end_date', '20220201', 'only get results before this date')
flags.DEFINE_string('facet', 'false', 'whether to get facet counts')
flags.DEFINE_string('facet_fields', None, 'values to show facet counts of')
flags.DEFINE_string('facet_filter', None, 'filters for facet counts')
flags.DEFINE_string('fl', None, 'what fields to get from each article')
flags.DEFINE_string('fq', None, 'filter query')
flags.DEFINE_integer('max_num_results', 20, 'max number of articles to get')
flags.DEFINE_integer('page', 0, 'what page number for results to begin on')
flags.DEFINE_integer('pages_per_update', 5, 'number of pages between logging updates')
flags.DEFINE_string('q', None, 'query string')
flags.DEFINE_string('sort', 'relevance', 'how to sort results')
flags.DEFINE_string(
    'url_base', 'https://api.nytimes.com/svc/search/v2/articlesearch.json?', 'base string for NYT Article Search API'
)
FLAGS = flags.FLAGS

def call_api(request_params, url_base):
    
    request_headers = {"Accept": "application/json"}
    return requests.get(url_base, headers=request_headers, params=request_params).json()


def execute(url_params, max_num_results, url_base, pages_per_update):
    
    request_params = url_params.copy()
    
    request = call_api(request_params, url_base)
    num_hits = request['response']['meta']['hits']
    total_results = min(max_num_results, num_hits)
    
    logging.info(f'there were {num_hits} hits for the query, retrieving {total_results} hits')

    results = [ request['response']['docs'] ][:min(10, max_num_results)]
    num_results_recorded = len(results[0])
    
    while num_results_recorded < total_results:
        
        request_params['page'] += 1
        request = call_api(request_params, url_base)
        
        num_results_to_take = min(10, max_num_results - num_results_recorded)
        request_result = request['response']['docs'][:num_results_to_take]
        time.sleep(6.1) # API has 10 calls / min limit
               
        results.append(request_result)
        num_results_recorded += num_results_to_take
        
        if pages_per_update is not None and request_params['page'] % pages_per_update == 0:
        
            r = request_params['page']
            l = r - pages_per_update
            
            logging.info(f'sucessfully retrieved pages {l} - {r}')
        
    # Concatenate each result json into single array
    return np.concatenate(results)

def api_call_results_to_df(api_call_results):
    
    # Hard-coded because these are really the only interesting things available from the API 
    pd_dict = {'snippet': [], 'main_headline': [], 'pub_date': [], 
               'source': [], 'print_headline': [], 'sub_headline': [],
               'type_of_material': [], 'word_count': []
              }
        
    for article in api_call_results:
        
        snippet = article['snippet']
        main_headline = article['headline']['main']
        pub_date = article['pub_date'][:10]
        source = article['source']
        print_headline = article['headline']['print_headline']
        sub_headline = article['headline']['sub']
        type_of_material = article['type_of_material']
        word_count = article['word_count']
        
        
        pd_dict['snippet'].append(snippet)
        pd_dict['main_headline'].append(main_headline)
        pd_dict['pub_date'].append(pub_date)
        pd_dict['source'].append(source)
        pd_dict['print_headline'].append(print_headline)
        pd_dict['sub_headline'].append(sub_headline)
        pd_dict['type_of_material'].append(type_of_material)
        pd_dict['word_count'].append(word_count)
        
    return pd.DataFrame(pd_dict)

def main(unused_argv):

    flags.mark_flag_as_required('q')
    flags.mark_flag_as_required('fl')
    flags.mark_flag_as_required('dest')   
    
    url_params = {
        'api-key': FLAGS.api_key,
        'begin_date': FLAGS.begin_date,
        'end_date': FLAGS.end_date, 
        'facet': FLAGS.facet,
        'facet_fields': FLAGS.facet_fields,
        'facet_filter': FLAGS.facet_filter,
        'fl': FLAGS.fl,
        'fq': FLAGS.fq,
        'page': FLAGS.page,
        'q': FLAGS.q,
        'sort': FLAGS.sort
    }

    dest = FLAGS.dest
    max_num_results = FLAGS.max_num_results
    pages_per_update = FLAGS.pages_per_update
    url_base = FLAGS.url_base
    
    api_call_results = execute(url_params, max_num_results, url_base, pages_per_update) # Get results from calls
    df = api_call_results_to_df(api_call_results) # Convert call results into df
    df.to_csv(path_or_buf=dest, header=list(df.columns), index=False) # Write df to dest
    
if __name__ == '__main__':
    
    try:
        app.run(main)
    except SystemExit:
        pass