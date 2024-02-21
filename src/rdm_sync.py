# native imports
import concurrent.futures
from datetime import date
import logging


# 3rd party imports
from notion.client import NotionClient

# lskm import
from notion_secrets import secrets
import rdm_scraper
from rdm_exclude import return_excluded_rdms as exclude_list

# logging setup
current_date = date.today()
logging.basicConfig(filename=f'./logs/log_{current_date}.log', encoding='utf-8', level=logging.DEBUG)

# threads on my machine
NUM_THREADS = 16

# client setup for Notion module. Uses notion secrets file.
# Base URL is for our notion instance.
CLIENT = NotionClient(token_v2= secrets()['session_token'])
BASE_URL = 'https://www.notion.so/riotgames/'

# these are the collection view Notion UUIDs. 
# Prod is live production. Dev is for development
PRODUCTS_COLLECTION_VIEW_PROD = '70cf378fbdd741cb9d72540ce61d0094?'\
    'v=aa863dfcef684d4bb0a58f5f193804e7'
TEAMS_COLLECTION_VIEW_PROD = 'f84cbf26f3cd4e3a8adae7fa61dc0206?'\
    'v=cb5473ceba2d473aa140fa7e60a9cca4'
PRODUCTS_COLLECTION_VIEW_DEV = '1ff035d114a74adfa15a2b7209537900?'\
    'v=fac89a0b0c7147c195c212e221137f2a'
TEAMS_COLLECTION_VIEW_DEV = '840d79a1174949698c080796ed46a1aa?'\
    'v=f69fc64270bf48e686951c4da1d06181'

#Columns are used to map column ids
PRODUCTS_COLUMNS = ['Product Name', 'Type', 'Owning Team(s)', 'Brief Description',
    'Homepage', 'Slack', 'Email', 'Pager Duty', 'Status', 'Riot Org URL (Click to Edit Record)',
    'rdm_rrn', 'Owning Business Unit', 'Owning Initiative']
TEAMS_COLUMNS = ['Team Name', 'Type', 'Homepage', 'Mission', 'Active?',
    'Parent', 'Children', 'Captain', 'Slack', 'Email', 'Support Channels',
    'Products', 'Riot Org URL (Click to Edit Record)', 'rdm_rrn', 'Owning Business Unit',
    'Owning Initiative']
#Iterators are used as the properties we want to set.
# todo: add the following properties:
#    'Homepage', 'Captain'
PRODUCTS_COLUMNS_ITERATOR = ['Product Name', 'Type', 'Brief Description',
    'Homepage', 'Slack', 'Email', 'Pager Duty', 'Status', 'Riot Org URL (Click to Edit Record)',
    'rdm_rrn']
TEAMS_COLUMNS_ITERATOR = ['Team Name', 'Type', 'Mission', 'Slack', 'Email',
    'Riot Org URL (Click to Edit Record)', 'rdm_rrn']
SKIP_LIST = ['Parent', 'Homepage', 'Children', 'Products', 'Owning Team(s)']
EXCLUDE_LIST = exclude_list()

def use_prod_or_dev_dbs(db_type = 'DEV'):
    if db_type == 'DEV':
        VIEW_DICT = {
            'PRODUCTS_VIEW': PRODUCTS_COLLECTION_VIEW_DEV,
            'TEAMS_VIEW': TEAMS_COLLECTION_VIEW_DEV
        }
    elif db_type == 'PROD':
        VIEW_DICT = {
                'PRODUCTS_VIEW': PRODUCTS_COLLECTION_VIEW_PROD,
                'TEAMS_VIEW': TEAMS_COLLECTION_VIEW_PROD
            }
    return(VIEW_DICT)

def return_properties(view, columns, skip=False):
    properties_dict = {}
    logging.info('Properties Values:')
    for elem in view.collection.get_schema_properties():
        if skip:
            if elem['name'] in SKIP_LIST:
                continue
        if elem['name'] in columns:
            properties_dict[elem['name']] = elem['id']
            logging.info(f'{elem['name']} = {elem['id']}')
    return(properties_dict)

def teams_scope_crawl(response):
    rdm_list = response['node']

    #process rdm_list
    rdm_dict = {}
    for row in rdm_list:
        rdm_dict[row['rrn']] = row
    graphql_list = response['graphql']['group']

    # process graph_ql list
    for row in graphql_list:
        rrn = row['_rdm_rrn']
        try:
            group = rdm_dict[rrn]
            group['scope'] = row['scope']
        except:
            group['scope'] = None
            logging.debug(f'Key lookup error in "rdm_dict" for rdm_rrn: {rrn}')

    # crawl through rdm_dict scopes and establish parent rdm_rrns
    for rdm_rrn in rdm_dict:
        group = rdm_dict[rdm_rrn]
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
    
    for rdm_rrn in rdm_dict:
        child = rdm_dict[rdm_rrn]
        child['parent_group'] = None

        try:
            parent_scope = child['parent_scope']
        except:
            continue

        for rrn in rdm_dict:
            parent = rdm_dict[rrn]
            try:
                if parent['scope'] == parent_scope:
                    child['parent_group'] = rrn
            except:
                continue
    
    return rdm_dict

def products_owner_crawl(response):
    rdm_list = response['node']

    #process rdm_list
    rdm_dict = {}
    for row in rdm_list:
        rdm_dict[row['rrn']] = row
    graphql_list = response['graphql']['product']

    for row in graphql_list:
        rrn = row['_rdm_rrn']
        owners_edge_list = row['groupOwnsProductEdge']
        owners_list = []
        for elem in owners_edge_list:
            group = elem['group']
            rdm_rrn = group['_rdm_rrn']
            owners_list.append(rdm_rrn)
        try:
            product = rdm_dict[rrn]
            product['owned_by'] = owners_list
        except:
            logging.debug(f'Key error for rdm_dict for rdm_rrn: {rrn}')
            continue

    return rdm_dict

def return_teams_dict():
    response = rdm_scraper.call_riotorg('teams')
    logging.info('Teams Dictioanry:')
    
    rdm_dict = teams_scope_crawl(response)

    logging.debug(response['debug'])

    teams_dict = {}
    for rdm_rrn in rdm_dict:
        group = rdm_dict[rdm_rrn]
        data = group['data']
        created_at = group['created_at']
        last_updated_at = group['updated_at']
        version = group['version']
        fullname = data['name']
        shortname = data['abbr']
        email_contact = data['contact_email']
        slack_url = f'https://riotgames.slack.com/archives/{data['contact_slack']}'
        slack_string = f'#{data['contact_slack']}'
        slack_contact = data['contact_slack']
        if slack_contact:
            slack_md = f'[{slack_string}]({slack_url})'
        else:
            slack_md = ''
        riotorg_url = data['group_url']
        riotorg_api_id = data['groups_api_id']
        mission = data['description']
        group_type = data['type']
        parent_group = group['parent_group']

        teams_dict[rdm_rrn] = {
            'rdm_rrn': rdm_rrn,
            'created_at': created_at,
            'last_updated_at': last_updated_at,
            'version': version,
            'Team Name': fullname,
            'shortname': shortname,
            'Email': email_contact,
            'slack_url': slack_url,
            'slack_string': slack_string,
            'Slack': slack_md,
            'slack_contact': slack_contact,
            'Riot Org URL (Click to Edit Record)': riotorg_url,
            'riotorg_api_id': riotorg_api_id,
            'Mission': mission,
            'Type': group_type,
            'parent_group': parent_group,
            'present_on_rdm_bool': True
        }
    logging.info(teams_dict)
    return teams_dict

def return_products_dict():
    response = rdm_scraper.call_riotorg('products')
    logging.info('Products Dictionary')

    rdm_dict = products_owner_crawl(response)

    logging.debug(response['debug'])
    
    products_dict = {}
    for rdm_rrn in rdm_dict:
        product = rdm_dict[rdm_rrn]
        data = product['data']
        created_at = product['created_at']
        last_updated_at = product['updated_at']
        version = product['version']
        fullname = data['name']
        email_contact = data.get('email', '')
        slack = data.get('slack', '')
        slack_url = f'https://riotgames.slack.com/archives/{slack}'
        slack_string = f'{slack}'
        slack_contact = slack
        if slack_contact:
            slack_md = f'[{slack_string}]({slack_url})'
        else:
            slack_md = ''
        pager_duty = data.get('pager_duty', '')
        riotorg_url = f'https://org.riotnet.io/product/{rdm_rrn}'
        product_url = product.get('product_url', '')
        description = data['description']
        product_type = product.get('type', 'Unknown')
        parent_rdm_rrns = product['owned_by']
        
        status = data.get('status', None)


        products_dict[rdm_rrn] = {
            'rdm_rrn': rdm_rrn,
            'created_at': created_at,
            'last_updated_at': last_updated_at,
            'version': version,
            'Product Name': fullname,
            'Type': product_type,
            'Brief Description': description,
            'Homepage': product_url,
            'Email': email_contact,
            'slack_url': slack_url,
            'slack_string': slack_string,
            'Slack': slack_md,
            'slack_contact': slack_contact,
            'Pager Duty': pager_duty,
            'Status': status,
            'Riot Org URL (Click to Edit Record)': riotorg_url,
            'parent_groups': parent_rdm_rrns,
            'present_on_rdm_bool': True,
            'status': status
        }
    logging.info(products_dict)
    return(products_dict)

def add_notion_uuids_to_dict(view, teams_dict):
    rows = view.collection.get_rows()
    if rows:
        for row in rows:
            properties = row.get_all_properties()
            rdm_rrn = properties['rdm_rrn']
            group = teams_dict[rdm_rrn]
            uuid = row.id
            group['notion_uuid'] = uuid
    else:
        # if this exception is raise, reset your v2 token.
        print('No rows, TOKEN RESET REQUIRED.')
        logging.exception('TOKEN RESET REQUIRED')
        raise Exception

def set_page_properties(page, properties_list, properties_dictionary, rdm_entry):
    try:
        for property in properties_list:
            try:
                page.set_property(properties_dictionary[property], rdm_entry[property])
            except Exception as e:
                print(e)
                if "520 Server Error" in str(e):
                    page.set_property(properties_dictionary[property], rdm_entry[property])
                else:
                    raise Exception
        return True
    except Exception as e:
        print(e)
        raise Exception

def update_page_properties(page, properties_list, properties_dictionary, rdm_entry):
    try:
        for property in properties_list:
            property_value = page.get_property(properties_dictionary[property])
            if property_value == rdm_entry[property]:
                print(f'Property not updated: {property}.')
                continue
            else:
                try:
                    page.set_property(properties_dictionary[property], rdm_entry[property])
                except Exception as e:
                    print(e)
                    if "520 Server Error" in str(e):
                        page.set_property(properties_dictionary[property], rdm_entry[property])
                    else:
                        raise Exception
        return True
    except Exception as e:
        print(e)
        raise Exception

def map_current_rows(row, teams_dict, ):
    pass
    
def update_teams(view, teams_dict):
    
    teams_properties_dict = return_properties(view, TEAMS_COLUMNS, skip=True)
    current_rows = view.collection.get_rows()

    #tracks which rdm_rrns have been processed
    tracking_dict = {}
    for rdm_rrn in teams_dict:
        tracking_dict[rdm_rrn] = False

    # iterate over current pages and update values
    def map_current_rows(row):
        rdm_rrn = row.get_property(teams_properties_dict['rdm_rrn'])
        if not rdm_rrn:
            return
        print(f'updating team {rdm_rrn}')
        if rdm_rrn == 'ROOT':
            teams_dict['ROOT'] = {'notion_uuid': row.id}
            return
        team = teams_dict[rdm_rrn]
        if team['Team Name'] == 'REDACTED':
            return
        tracking_dict[rdm_rrn] = update_page_properties(
            row,
            TEAMS_COLUMNS_ITERATOR, 
            teams_properties_dict,
            teams_dict[rdm_rrn]
        )
        team['notion_uuid'] = row.id
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(map_current_rows, current_rows)

    # iterate over every rdm_rrn that is false in the tracking dictionary 
    # and create a new page
    def map_teams_dict(rdm_rrn):
    #for rdm_rrn in teams_dict:
        if not tracking_dict[rdm_rrn]:
            team = teams_dict[rdm_rrn]
            if team['Team Name'] == 'REDACTED':
                return
            print(f'creating team {rdm_rrn}')
            new_row = view.collection.add_row()
            tracking_dict[rdm_rrn] = set_page_properties(
                new_row,
                TEAMS_COLUMNS_ITERATOR, 
                teams_properties_dict, 
                teams_dict[rdm_rrn]
            )
            team['notion_uuid'] = new_row.id
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(map_teams_dict, tracking_dict)

    current_rows = view.collection.get_rows()
    teams_properties_dict = return_properties(view, TEAMS_COLUMNS)

    def map_hierarchy(row):
    #for row in current_rows:
        rdm_rrn = row.get_property(teams_properties_dict['rdm_rrn'])
        if not rdm_rrn or rdm_rrn == 'ROOT':
            #continue
            return
        team = teams_dict[rdm_rrn]
        parent_team_rdm_rrn = team['parent_group']
        if parent_team_rdm_rrn:
            parent_team = teams_dict[parent_team_rdm_rrn]
            parent_team_notion_uuid = parent_team['notion_uuid']
            current_parent = row.get_property(teams_properties_dict['Parent'])
            current_parent_notion_uuid = current_parent[0].id
            if parent_team_notion_uuid == current_parent_notion_uuid:
                #continue
                return
            elif parent_team['Team Name'] == 'REDACTED':
                #continue
                return
            else:
                print(f'mapping teams: {team['Team Name']} to {parent_team['Team Name']}')
                row.set_property(teams_properties_dict['Parent'], parent_team['notion_uuid'])
        else:
            root = teams_dict['ROOT']
            print(f'mapping {team['Full Name']} to ROOT')
            row.set_property(teams_properties_dict['Parent'], root['notion_uuid'])
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(map_hierarchy, current_rows)

def update_products(view, products_dict, teams_dict):
    products_properties_dict = return_properties(view, PRODUCTS_COLUMNS, skip=False)
    current_rows = view.collection.get_rows()
    tracking_dict = {}

    for rdm_rrn in products_dict:
        tracking_dict[rdm_rrn] = False

    print(f'Updating existing products')
    def map_current_rows(row):
    #for row in current_rows:
        rdm_rrn = row.get_property(products_properties_dict['rdm_rrn'])
        if not rdm_rrn or rdm_rrn in EXCLUDE_LIST:
            #continue
            return
        print(f'updating product {rdm_rrn}')
        product = products_dict[rdm_rrn]
        if product['Product Name'] == 'REDACTED':
            #continue
            return
        tracking_dict[rdm_rrn] = update_page_properties(
            row,
            PRODUCTS_COLUMNS_ITERATOR, 
            products_properties_dict,
            products_dict[rdm_rrn]
        )
        product['notion_uuid'] = row.id
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(map_current_rows, current_rows)

    # iterate over every rdm_rrn that is false in the tracking dictionary 
    # and create a new page
    print(f'Creating new products.')
    def map_products_dict(rdm_rrn):
        if not tracking_dict[rdm_rrn]:
            product = products_dict[rdm_rrn]
            if product['Product Name'] == 'REDACTED':
                return
            print(f'creating product {rdm_rrn}')
            new_row = view.collection.add_row()
            tracking_dict[rdm_rrn] = set_page_properties(
                new_row,
                PRODUCTS_COLUMNS_ITERATOR, 
                products_properties_dict, 
                products_dict[rdm_rrn]
            )
            product['notion_uuid'] = new_row.id

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(map_products_dict, products_dict)

    current_rows = view.collection.get_rows()
    products_properties_dict = return_properties(view, PRODUCTS_COLUMNS)

    def map_hierarchy(row):
    #for row in current_rows:
        rdm_rrn = row.get_property(products_properties_dict['rdm_rrn'])
        if not rdm_rrn or rdm_rrn in EXCLUDE_LIST:
            #continue
            return
        print(f'mapping {rdm_rrn}')
        product = products_dict[rdm_rrn]
        parent_team_rdm_rrns = product['parent_groups']
        parent_team_notion_uuids = []
        for rdm_rrn in parent_team_rdm_rrns:
            team = teams_dict[rdm_rrn]
            notion_uuid = team['notion_uuid']
            parent_team_notion_uuids.append(notion_uuid)
        if parent_team_notion_uuids:
            print(f'mapping product {product['Product Name']} to team {team['Team Name']}')
            row.set_property(products_properties_dict['Owning Team(s)'], parent_team_notion_uuids)
        print(f'Finished mapping {rdm_rrn}')
    print(f'DONE MAPPPING TEAMS')
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(map_hierarchy, current_rows)

def map_product_owning_teams(teams_view, products_view, products_dict):
    teams_rows = teams_view.collection.get_rows()
    teams_properties = return_properties(teams_view, TEAMS_COLUMNS)
    teams_mapping = {}

    def map_teams_rows(row):
    #for row in teams_rows:
        notion_uuid = row.id
        rdm_rrn = row.get_property(teams_properties['rdm_rrn'])
        if rdm_rrn:
            teams_mapping[rdm_rrn] = notion_uuid

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(map_teams_rows, teams_rows)

    del teams_rows

    products_rows = products_view.collection.get_rows()
    products_properties = return_properties(products_view, PRODUCTS_COLUMNS)

    def map_product_rows(row):
    #for row in products_rows:
        rdm_rrn = row.get_property(products_properties['rdm_rrn'])
        #print(rdm_rrn)
        if rdm_rrn:
            product = products_dict[rdm_rrn]
            parent_rdm_rrns = product['parent_groups']
            parent_uuids = []
            for rrn in parent_rdm_rrns:
                parent_uuids.append(teams_mapping[rrn])
            #print(parent_uuids)
            if parent_uuids:
                row.set_property(products_properties['Owning Team(s)'], parent_uuids)
                print(f'mapping product {row.title_plaintext} to teams: {parent_uuids}')
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(map_product_rows, products_rows)

def update_relations(use_prod_or_dev):
    view_dict = use_prod_or_dev_dbs(use_prod_or_dev)
    TEAMS_COLLECTION_VIEW = view_dict['TEAMS_VIEW']
    PRODUCTS_COLLECTION_VIEW = view_dict['PRODUCTS_VIEW']
    teams_view = CLIENT.get_collection_view(BASE_URL + TEAMS_COLLECTION_VIEW)
    products_view = CLIENT.get_collection_view(BASE_URL + PRODUCTS_COLLECTION_VIEW)

    teams_properties = return_properties(teams_view, TEAMS_COLUMNS)
    products_properties = return_properties(products_view, PRODUCTS_COLUMNS)

    exlude = ['studio', 'pillar', 'company']

    teams_rows = teams_view.collection.get_rows()
    teams_dict = {}
    #def map_rows(row):
    for row in teams_rows:
        if not row.get_property(teams_properties['rdm_rrn']):
            continue
            #return
        parent = row.get_property(teams_properties['Parent'])
        try:
            parent_id = parent[0].id
            if not parent_id:
                parent_id = None
        except:
            parent_id = None
        team_type = row.get_property(teams_properties['Type'])
        row_id = row.id
        title = row.title_plaintext

        teams_dict[row_id] = {
            'parent': parent_id,
            'type': team_type,
            'owning_initiative': None,
            'owning_bu': None,
            'title': title
        }
        #print(f'done mapping {row.title_plaintext}')
        #return
    
    '''with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(map_rows, teams_rows)'''
    if not teams_dict:
        raise Exception
    print('done mapping teams_dict')

    def find_initative(id, return_id = None):
        dict_entry = teams_dict[id]
        team_type = dict_entry['type']
        parent_id = dict_entry['parent']
        if team_type == 'business unit':
            print(f'intiative lookup found BU. Returning None.')
            return None
        if team_type in exlude:
            print('team type is in exlude. Returning None.')
            return None
        if not parent_id:
            print(f'parent_id is None. Returning None.')
            return None
        if dict_entry['title'] == 'Riot Games':
            print(f'title is "Riot Games". Returning None')
            return None
        if team_type == 'initiative':
            print(f'returning initiative id {id}')
            return_id = id
            return return_id
        else:
            print(f'finding parent intiative of {id}')
            return_id = find_initative(parent_id)
        print(return_id)
        return return_id
    
    def find_BU(id, return_id = None):
        dict_entry = teams_dict[id]
        team_type = dict_entry['type']
        parent_id = dict_entry['parent']
        if team_type in exlude:
            print('team type is in exlude. Returning None.')
            return None
        if not parent_id:
            print(f'parent_id is None. Returning None.')
            return None
        if dict_entry['title'] == 'Riot Games':
            print(f'title is "Riot Games". Returning None')
            return None
        if team_type == 'business unit':
            print(f'returning BU id {id}')
            return_id = id
            return return_id
        else:
            print(f'finding parent BU of {id}')
            return_id = find_BU(parent_id)
        print(return_id)
        return return_id
    
    count = 0
    for id in teams_dict:
        entry = teams_dict[id]
        if entry['type'] in exlude:
            print(f'excluding {id}. Continuing.')
            count += 1
            continue
        owning_initiative = find_initative(id)
        owning_bu = find_BU(id)
        entry['owning_initiative'] = owning_initiative
        entry['owning_bu'] = owning_bu
        count += 1


    print(f'mapping teams')
    def map_rows(row):
    #for row in teams_rows:
        
        team_type = row.get_property(teams_properties['Type'])
        if team_type in exlude:
            #continue
            return
        id = row.id
        try:
            entry = teams_dict[id]
        except Exception as e:
            print(e)
            #continue
            return
        owning_initiative = entry['owning_initiative']
        owning_bu = entry['owning_bu']
        #print(f'setting values for {id}')
        if owning_initiative:
            row.set_property(teams_properties['Owning Initiative'], owning_initiative)
        if owning_bu:
            row.set_property(teams_properties['Owning Business Unit'], owning_bu)
    
    #with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
    #    executor.map(map_rows, teams_rows)

    products_rows = products_view.collection.get_rows()
    print(f'mapping products')
    def map_rows(row):
    #for row in products_rows:
        owning_team = row.get_property(products_properties['Owning Team(s)'])
        try:
            owning_team_id = owning_team[0].id
        except:
            #continue
            return
        try:
            entry = teams_dict[owning_team_id]
            owning_initiative = entry['owning_initiative']
            owning_bu = entry['owning_bu']
            if owning_initiative:
                row.set_property(products_properties['Owning Initiative'], owning_initiative)
            if owning_bu:
                row.set_property(products_properties['Owning Business Unit'], owning_bu)
        except Exception as e:
            print(e)
            #continue
            return
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(map_rows, products_rows)
            

def update_pages(use_prod_or_dev):
    view_dict = use_prod_or_dev_dbs(use_prod_or_dev)
    TEAMS_COLLECTION_VIEW = view_dict['TEAMS_VIEW']
    PRODUCTS_COLLECTION_VIEW = view_dict['PRODUCTS_VIEW']

    print('updating teams')
    teams_view = CLIENT.get_collection_view(BASE_URL + TEAMS_COLLECTION_VIEW)
    teams_dict = return_teams_dict()
    update_teams(teams_view, teams_dict)

    print('updating products')
    products_view = CLIENT.get_collection_view(BASE_URL + PRODUCTS_COLLECTION_VIEW)
    products_dict = return_products_dict()
    update_products(products_view, products_dict, teams_dict)
    print('done')

def testing():

    products_dict = return_products_dict()
    for key in products_dict:
        print(products_dict[key])
    test ='test'


def main():
    update_pages(use_prod_or_dev='DEV')

if __name__ == '__main__':
    main()
