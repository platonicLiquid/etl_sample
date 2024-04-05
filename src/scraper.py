from riotorg_apis_wrapper import riotorg_api

from rdm_secrets import secrets as rdm_secrets
from war_groups_secrets import secrets as war_secrets
from graphql_query_strings import return_query_string

def call_riotorg(teams_or_products, page=None):
    #test to make sure right parameter is given
    if teams_or_products == 'teams' or teams_or_products == 'team':
        api = 'war_graphql'
        token = war_secrets()['token']
    elif teams_or_products == 'products' or teams_or_products == 'product':
        api = 'rdm_graphql'
        token = rdm_secrets()['token']
    elif teams_or_products == 'rdm_teams':
        api = 'rdm_graphql'
        token = rdm_secrets()['token']
    else:
        print('WRONG value given, must be "teams" or "products".')
        raise Exception

    query_string = return_query_string(teams_or_products, page)
    graphql_response = riotorg_api(api, token=token)
    try:
        graphql_response.call_graphql(query_string=query_string)
    except Exception as e:
        print(e)
        raise Exception

    debug_dict = {
        'teams_or_products': teams_or_products,
    }

    return_dict = {
        'graphql': graphql_response.response,
        'debug': debug_dict
    }
    
    return return_dict
