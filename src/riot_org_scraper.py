import requests
import json

import rdm_wrapper
import pandas as pd


from rdm_secrets import secrets


TOKEN = secrets()['token']


def call_riotorg(teams_or_products, debug=False):
    #test to make sure right parameter is given
    if teams_or_products == 'teams':
        kind = 'group'
        name = 'node_kind'
    elif teams_or_products == 'products':
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
    return_body = []

    #we force the value back into an integer after dividing it
    for page_number in range(int(total_items/max_page_size) + zero_start_increment):
        page_size = min(total_items, max_page_size)
        total_items = total_items - max_page_size
        full_response = rdm_wrapper.rdm_api(name, TOKEN, kind, pagesize=page_size, page=page_number)
        full_response.call_api()
        if not full_response.response.status_code == 200:
            print(f'{full_response.response.status_code}, {full_response.response.reason}')
            raise Exception
        full_response_text = json.loads(full_response.response.text)
        return_body = return_body + full_response_text['body']
    
    if debug:
        debug_dict = {
            'teams_or_products': teams_or_products,
            'status_code': full_response.response.status_code,
            'response_text': response_text,
            'options': options,
            'total_item': total_items
        }
        return([return_body, debug_dict])

    return return_body


'''# TODO return users function to map users to teams
def return_users():
    response = requests.get(f'{ROOT_URL + TEAMS_API}')
    if not response.status_code == 200:
        print(f'{response.status_code}, {response.reason}')
        raise Exception
    resonse_loads = json.load(response.text)
    headcount = resonse_loads['options']['totalitems']
    if headcount > 10000:
        range_count = 10000
    else:
        range_count = headcount


def create_json_files():
    # creates json files
    teams = str(call_riotorg(TEAMS_API))
    products = str(call_riotorg(PRODUCTS_API))

    with open('./riotorgJSON/teams.json', 'w', encoding='utf-8') as json_file:
        json_file.write(teams)
    
    with open('./riotorgJSON/products.json', 'w', encoding='utf-8') as json_file:
        json_file.write(products)
    
    return(True)


def json_diff():
    old_teams_df = pd.read_json('./riotorgJSON/teams.json')
    old_products_df = pd.read_json('./riotorgJSON/products.json')
    
    current_teams_df = pd.read_json(call_riotorg(TEAMS_API))
    current_products_df = pd.read_json(call_riotorg(PRODUCTS_API))'''

