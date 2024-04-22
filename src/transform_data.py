#native imports
import logging
from datetime import date

#local imports
import group_column_mapping
import product_column_mapping
from etl_classes import data_obj

# static strings
GROUPS_URL_PREFIX = 'https://org.riotnet.io/teams/'
PRODUCTS_URL_PREFIX = 'https://org.riotnet.io/product/'
PRODUCTS_OWNING_TEAM_URL_PREFIX = 'https://teams.riotgames.com/teams/'

#logging
current_date = date.today()
logging.basicConfig(
        filename=f'./logs/transformations_log_{current_date}.log',
        encoding='utf-8',
        level=logging.ERROR
    )

#globals
owning_initative_str = 'Owning Initiative'
owning_bu_str = 'Owning Business Unit'

# universal functions
def map_data_to_notion_columns(dict_to_map, data_schema_dict):
    return_dict = {}
    
    for elem in dict_to_map:
        entry = {}
        data = dict_to_map[elem]
        for key in data_schema_dict:
            new_key_name = data_schema_dict[key]
            entry[new_key_name] = data[key]
        return_dict[elem] = entry
    
    return return_dict

def format_slack_string(slack_channel):
    if type(slack_channel) == list:
        slack_channel = slack_channel[0]
    if '#' in slack_channel:
        slack_channel = slack_channel.replace('#', '')
    slack_url = f'https://riotgames.slack.com/archives/{slack_channel}'
    slack_string = f'#{slack_channel}'
    #turns our slack string into a useable markdown link [string](url)
    slack_md = f'[{slack_string}]({slack_url})'
    return slack_md

def format_support_channels(support_channels):
    support_str = ''
    for support in support_channels:
        slack_url = format_slack_string(support)
        support_str = support_str + slack_url + '\n'

    support_str = support_str.rstrip()

    return support_str

def format_emails(emails):
    email_str = ''

    for email in emails:
        email_str = email_str + email + '\n'
    
    email_str = email_str.rstrip()

    return email_str

## Teams Transformations
def worker_trasformation(leadership_list):
    # function takes the work list returned by an edge and extras emails from the
    # provided users
    return_str = ''

    for elem in leadership_list:
        description = elem['description']
        if description == 'team captain':
            worker_meta = elem['workerMeta']
            name = worker_meta['name']
            username = worker_meta['username']
            return_str = f'{name} ({username})\n'
    return_str = return_str.rstrip()

    return return_str

def meta_transformation(meta_dict, group_dict):
    mission_statement = meta_dict['missionStatement']
    if not mission_statement:
        mission_statement = None
    contacts = meta_dict['contact']
    if not contacts:
        slack = None
        email = None
        support = None
    else:
        slack = contacts['slack']
        email = contacts['email']
        support = contacts['support']
        formatting_list = [slack, email, support]
        for elem in formatting_list:
            if not elem:
                elem = None
    group_dict['missionStatement'] = mission_statement
    group_dict['slack'] = slack
    group_dict['email'] = email
    group_dict['support'] = support

    return

def create_group_dictionary(groups):
    return_dict = {}
    groups_iterator = group_column_mapping.group_data_columns_iterator()

    for group in groups:
        workday_id = group['workdayID']
        if not workday_id:
            continue

        group_dict = {}

        leadership_list = group['leadershipAssignment']
        meta_dict = group['meta']

        for elem in groups_iterator:
            group_dict[elem] = group[elem]

        #transform groupsID
        if group_dict['groupsID']:
            group_dict['group_url'] = GROUPS_URL_PREFIX + group['groupsID']
        else:
            group_dict['group_url'] = None

        if not leadership_list:
            team_captains = []
        else:
            team_captains = worker_trasformation(leadership_list)
        group_dict['team_captains'] = team_captains

        meta_transformation(meta_dict, group_dict)

        return_dict[workday_id] = group_dict

    return return_dict

def format_data(transformed_data):
    return_dict = {}

    for workday_id in transformed_data:
        data = transformed_data[workday_id]
        data['Active?'] = 'Active'
        team_obj = data_obj(data, 'workdayID')
        team_obj.name = data['Team Name']
        if data['Slack']:
            data['Slack'] = format_slack_string(data['Slack'])
        if data['Support Channels']:
            data['Support Channels'] = format_support_channels(data['Support Channels'])
        if data['Email']:
            data['Email'] = format_emails(data['Email'])
        return_dict[workday_id] = team_obj

    return return_dict

def teams_scope_crawl(teams_dict):
    # crawl through rdm_dict scopes and establish parent workday_ids
    for workday_id in teams_dict:
        group_obj = teams_dict[workday_id]
        group = group_obj.data_transformed
        scope = group['scope']
        if not scope:
            continue
        split_list = scope.split('.')
        split_list_len = len(split_list)
        zero_start_modifier = 1
        partition_str = f'.{split_list[split_list_len - zero_start_modifier]}'
        rpartiontion_scope = scope.rpartition(partition_str)
        parent_scope = rpartiontion_scope[0]

        group['parent_scope'] = parent_scope
    
    for workday_id in teams_dict:
        child_obj = teams_dict[workday_id]
        child = child_obj.data_transformed
        child['parent_group'] = None

        try:
            parent_scope = child['parent_scope']
        except:
            continue

        for workday_id_iteration in teams_dict:
            parent_obj = teams_dict[workday_id_iteration]
            parent = parent_obj.data_transformed
            try:
                if parent['scope'] == parent_scope:
                    child['parent_group'] = workday_id_iteration
            except:
                continue

def find_bu(team, teams_dict):
    exclude_list = ['Studio', 'Pillar', 'Company']

    parent_workday_id = team['parent_group']
    try:
        parent_obj = teams_dict[parent_workday_id]
        parent = parent_obj.data_transformed
        parent_type = parent['Type']
    except:
        return None
    if parent_type in exclude_list:
        return None
    if parent_type == 'Business Unit':
        return parent_workday_id
    else:
        parent_workday_id = find_bu(parent, teams_dict)
        return parent_workday_id

def find_initiative(team, teams_dict):
    exclude_list = ['Studio', 'Pillar', 'Company']

    parent_workday_id = team['parent_group']
    try:
        parent_obj = teams_dict[parent_workday_id]
        parent = parent_obj.data_transformed
        parent_type = parent['Type']
    except:
        return None
    if parent_type in exclude_list:
        return None
    if parent_type == 'Initiative':
        return parent_workday_id
    else:
        parent_workday_id = find_initiative(parent, teams_dict)
        return parent_workday_id

def teams_bu_and_initiative_crawl(teams_dict):
    exclude_list = ['Studio', 'Pillar', 'Company']
    for workday_id in teams_dict:
        team_obj = teams_dict[workday_id]
        team = team_obj.data_transformed
        team_type = team['Type']
        if team_type in exclude_list:
            team[owning_initative_str] = None
            team[owning_bu_str] = None
            continue
        parent_bu_workday_id = find_bu(team, teams_dict)
        team[owning_bu_str] = parent_bu_workday_id
        parent_initiative_workday_id = find_initiative(team, teams_dict)
        team[owning_initative_str] = parent_initiative_workday_id

def transform_war_group_data(response):
    war_group_dict = create_group_dictionary(response)

    data_schema_dict = group_column_mapping.data_schema_names_mapped_to_notion_column_names_dict()
    transformed_data = map_data_to_notion_columns(war_group_dict, data_schema_dict)
    teams_dict = format_data(transformed_data)

    teams_scope_crawl(teams_dict)
    teams_bu_and_initiative_crawl(teams_dict)

    return teams_dict

## Products Transformations
def create_teams_scope_dict(teams_dict):
    return_dict = {}

    for workday_id in teams_dict:
        team_obj = teams_dict[workday_id]
        data = team_obj.data_transformed
        if data['Team Name'] == 'Riot Games':
            continue
        scope = data['scope']
        return_dict[scope] = workday_id

    return return_dict

def extract_workday_id(edge_list, product_dict, war_scope_dict):
    owning_group_workday_ids = []

    for elem in edge_list:
        owning_group = elem['group']
        owning_group_scope = owning_group['scope']
        owning_group_scope = owning_group_scope.replace('riot.', '')
        workday_id = war_scope_dict.get(owning_group_scope, None)
        if not workday_id:
            continue
        else:
            owning_group_workday_ids.append(workday_id)
    
    product_dict['owning_group_workday_ids'] = owning_group_workday_ids

def add_data_to_product_dict(product, product_dict):
    products_iterator = product_column_mapping.products_data_columns_iterator()

    for elem in products_iterator:
        product_dict[elem] = product[elem]

    #fix leading underscore
    product_dict['rdm_rrn'] = product_dict.pop('_rdm_rrn')

    rdm_rrn = product_dict['rdm_rrn']
    product_dict['riot_org_url'] = PRODUCTS_URL_PREFIX + rdm_rrn

def create_product_dictionary(products, node_data, teams_dict):
    return_dict = {}

    war_scope_dict = create_teams_scope_dict(teams_dict)
    
    for product in products:
        product_dict = {}

        rdm_rrn = product['_rdm_rrn']

        add_data_to_product_dict(product, product_dict)

        owning_group_list = product['groupOwnsProductEdge']
        extract_workday_id(owning_group_list, product_dict, war_scope_dict)
        
        return_dict[rdm_rrn] = product_dict
    
    for row in node_data:
        rdm_rrn = row['rrn']
        rrn_dict = return_dict.get(rdm_rrn, None)
        data = row['data']
        notion_url = data.get('notion_url', None)
        if notion_url:
            rrn_dict['notion_url'] = notion_url
        elif rrn_dict:
            rrn_dict['notion_url'] = None

    return return_dict

def format_product_data(transformed_data):
    return_dict = {}

    for rdm_rrn in transformed_data:
        data = transformed_data[rdm_rrn]
        product_obj = data_obj(data, 'rdm_rrn')
        product_obj.name = data['Product Name']
        data['Active?'] = 'Active'
        if data['Slack']:
            data['Slack'] = format_slack_string(data['Slack'])
        if data['Pager Duty'] == None or data['Pager Duty'] == 'a':
            data['Pager Duty'] = ''
        return_dict[rdm_rrn] = product_obj

    return return_dict

def transform_rdm_product_data(response_dict, teams_dict):
    node_data = response_dict['products_data_node_raw']
    graphql_data_raw = response_dict['products_data_graphql_raw']
    graphql_data = graphql_data_raw['product']

    product_dict = create_product_dictionary(graphql_data, node_data, teams_dict)

    data_schema_dict = product_column_mapping.data_schema_names_mapped_to_notion_column_names_dict()
    transformed_data = map_data_to_notion_columns(product_dict, data_schema_dict)
    products_dict = format_product_data(transformed_data)

    return products_dict
