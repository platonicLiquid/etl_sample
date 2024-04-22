from datetime import date
import logging

from scraper import api_caller

current_date = date.today()
logging.basicConfig(
        filename=f'./logs/data_extraction_log_{current_date}.log',
        encoding='utf-8',
        level=logging.ERROR
    )


def extract_teams_data_from_war_groups():
    return_data = []
    end_found = False
    page = 1
    api_name = 'war_graphql'
    caller = api_caller(api_name, page=page)

    while not end_found:
        caller.riotorg_obj.call_graphql(caller.query_string)
        response = caller.riotorg_obj.gql_response
        data = response['allGroups']['data']
        if not data:
            end_found = True
            continue
        for elem in data:
            if elem['workdayID'] == None:
                continue
            else:
                return_data.append(elem)
        page += 1
        caller.update_query_string(caller.alias, page)

    return return_data

def extract_products_data_from_rdm_graphql():
    api_name = 'rdm_graphql'
    caller = api_caller(api_name)
    caller.riotorg_obj.call_graphql(caller.query_string)
    return_data = caller.riotorg_obj.gql_response

    return return_data

def extract_products_data_from_rdm_node():
    api_name = 'rdm_node'
    page = 1

    caller = api_caller(api_name)
    caller.riotorg_obj.call_rdm_node(page)
    options_ping = caller.riotorg_obj.rdm_response.json()
    total_pages = options_ping['options']['totalpages']

    return_data = []

    while page <= total_pages:
        caller.riotorg_obj.call_rdm_node(page)
        data = caller.riotorg_obj.rdm_response.json()
        body = data['body']

        for row in body:
            return_data.append(row)
        
        page += 1
    
    return return_data
