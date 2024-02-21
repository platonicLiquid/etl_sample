import requests
import json

import rdm_wrapper
import pandas as pd


from rdm_secrets import secrets
from graphql_query_strings import return_query_string

TOKEN = secrets()['token']




def call_riotorg(teams_or_products):
    #test to make sure right parameter is given
    if teams_or_products == 'teams' or teams_or_products == 'team':
        kind = 'group'
        name = 'node_kind'
        
    elif teams_or_products == 'products' or teams_or_products == 'product':
        kind = 'product'
        name = 'node_kind'
    else:
        print('WRONG value given, must be "teams" or "products".')
        raise Exception

    r = rdm_wrapper.rdm_api(name, TOKEN, kind=kind)
    r.call_api()
    if not r.response.status_code == 200:
        print(f'{r.response.status_code}, {r.response.reason}')
        raise Exception
    response_text = json.loads(r.response.text)
    options = response_text['options']
    total_items = options['totalitems']

    max_page_size = 1000
    zero_start_increment = 1
    node_return_body = []

    #we force the value back into an integer after dividing it
    for page_number in range(int(total_items/max_page_size) + zero_start_increment):
        page_size = min(total_items, max_page_size)
        total_items = total_items - max_page_size
        full_response = rdm_wrapper.rdm_api(name, TOKEN, kind, pagesize=page_size, page=page_number)
        full_response.call_api()
        if not full_response.response.status_code == 200:
            print(f'STATUS CODE: {full_response.response.status_code}. \nREASON: {full_response.response.reason}.')
            raise Exception
        full_response_text = json.loads(full_response.response.text)
        node_return_body = node_return_body + full_response_text['body']
    

    query_string = return_query_string(teams_or_products)
    graphql_response = rdm_wrapper.rdm_api('graphql', token=TOKEN)
    try:
        graphql_response.call_graphql(query_string=query_string)
    except Exception as e:
        print(e)
        raise Exception

    debug_dict = {
        'teams_or_products': teams_or_products,
        'status_code': full_response.response.status_code,
        'response_text': response_text,
        'options': options,
        'total_item': total_items
    }

    return_dict = {
        'node': node_return_body,
        'graphql': graphql_response.response,
        'debug': debug_dict
    }
    
    return return_dict
