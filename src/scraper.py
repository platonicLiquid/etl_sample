from riot_org_api_wrapper import riotorg_api

from rdm_secrets import secrets as rdm_secrets
from war_groups_secrets import secrets as war_secrets
from graphql_query_strings import return_query_string

def api_definitions(page=None):
    war_token = war_secrets()['token']
    rdm_token = rdm_secrets()['token']

    rdm_graphql = {
        'name': 'rdm_graphql',
        'alias': 'products',
        'token': rdm_token,
        'query_string': return_query_string('products')
    }
    rdm_node = {
        'name': 'rdm_node',
        'token': rdm_token,
        'node': 'product'
    }
    war_graphql = {
        'name': 'war_graphql',
        'alias': 'teams',
        'token': war_token,
        'query_string': return_query_string('teams', page)
    }

    definition_dict = {
        'rdm_graphql': rdm_graphql,
        'rdm_node': rdm_node,
        'war_graphql': war_graphql
    }

    return definition_dict

class api_caller:
    def __init__(self, call_name, page=None):
        self.name = call_name
        api_definition = api_definitions(page)[call_name]
        self.token = api_definition.get('token', None)
        self.node = api_definition.get('node', None)
        self.query_string = api_definition.get('query_string', None)
        self.alias = api_definition.get('alias', None)
        self.riotorg_obj = riotorg_api(name=self.name, token=self.token, node=self.node)
    
    def update_query_string(self, name_type, page):
        self.query_string = return_query_string(name_type, page)
        
