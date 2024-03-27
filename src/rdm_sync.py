# native imports
import concurrent.futures
from datetime import date
import logging
import csv

# 3rd party imports
from notion.client import NotionClient
import pandas

# lskm import
from notion_secrets import secrets
from scraper import call_riotorg
from transform_data import transform_war_group_data, transform_rdm_product_data
from rdm_classes import rdm_obj, dry_run
from pickle_jars import pickle_dictionary, open_pickle_jar

# logging setup
current_date = date.today()
logging.basicConfig(filename=f'./logs/log_{current_date}.log', encoding='utf-8', level=logging.DEBUG)

# threads on my machine


# client setup for Notion module. Uses notion secrets file.
# Base URL is for our notion instance.
CLIENT = NotionClient(token_v2= secrets()['session_token'])
BASE_URL = 'https://www.notion.so/riotgames/'



def write_to_csv(title, list):
    df = pandas.DataFrame(list)
    df.to_csv(f'./src/validation_files/{current_date}_{title}.csv', index=False)



def return_rdm_teams_dict(teams_dict):
    return_dict = {}

    for workday_id in teams_dict:
        team_obj = teams_dict[workday_id]
        rdm_rrn = team_obj.data_transformed['rdm_rrn']
        return_dict[rdm_rrn] = team_obj

    return return_dict

def add_notion_uuids_to_dict(view, teams_dict):
    rows = view.collection.get_rows()
    if rows:
        for row in rows:
            properties = row.get_all_properties()
            workday_id = properties['workdayID']
            group = teams_dict[workday_id]
            uuid = row.id
            group['notion_uuid'] = uuid
    else:
        # if this exception is raised, reset your v2 token.
        print('No rows, TOKEN RESET REQUIRED.')
        logging.exception('TOKEN RESET REQUIRED')
        raise Exception

def get_notion_meta_data(notion_page, rdm_obj):
    try:
        notion_uuid = notion_page.id
        notion_title = notion_page.title_plaintext
        rdm_obj.notion_uuid = notion_uuid
        rdm_obj.notion_title = notion_title
    except:
        raise Exception

def set_page_properties(page, properties_list, properties_dictionary, rdm_obj, dry_run_bool):
    try:
        get_notion_meta_data(page, rdm_obj)
        rdm_obj.page_properties_set_bool = False
        for property in properties_list:
            try:
                rdm_entry = rdm_obj.data_transformed
                if dry_run_bool:
                    change_list = [property, rdm_entry[property]]
                    rdm_obj.dry_run_change_list.append(change_list)
                    continue
                page.set_property(properties_dictionary[property], rdm_entry[property])
            except Exception as e:
                print(e)
                if "520 Server Error" in str(e):
                    page.set_property(properties_dictionary[property], rdm_entry[property])
                else:
                    logging.error(f' for notion page id: {page.id}, property: {property}.')
                    raise Exception
        rdm_obj.page_properties_set_bool = True
        return True
    except Exception as e:
        print('Error')
        print(e)
        raise Exception

def update_page_properties(page, properties_list, properties_dictionary, rdm_obj, dry_run_bool):
    try:
        get_notion_meta_data(page, rdm_obj)
        rdm_obj.page_properties_set_bool = False
        for property in properties_list:
            
            property_value = page.get_property(properties_dictionary[property])
            rdm_entry = rdm_obj.data_transformed
            if property_value == rdm_entry[property]:
                #print(f'Property not updated: {property}.')
                continue
            else:
                try:
                    if dry_run_bool:
                        change_list = [property, rdm_entry[property]]
                        rdm_obj.dry_run_change_list.append(change_list)
                        continue
                    page.set_property(properties_dictionary[property], rdm_entry[property])
                    print(f'Property set: {property}.')
                except Exception as e:
                    print(f'printing error: {e}')
                    if "520 Server Error" in str(e):
                        page.set_property(properties_dictionary[property], rdm_entry[property])
                    else:
                        raise Exception
        rdm_obj.page_properties_set_bool = True
        return True
    except Exception as e:
        print(f'printing error: {e}')
        raise Exception

def set_active_to_false(page, properties_dictionary):
    try:
        page.set_property(properties_dictionary['Active'], 'False')
    except Exception as e:
        print(e)
        if "520 Server Error" in str(e):
            page.set_property(properties_dictionary['Active'], 'False')
        else:
            logging.error(f'Unable to set Active to False for page: {BASE_URL + page.id}')
            raise Exception

def update_current_rows_teams(teams_dict, teams_properties_dict, current_rows, dry_run_obj):
    #tracks which workday ids have been processed
    tracking_dict = {}
    for workday_id in teams_dict:
        tracking_dict[workday_id] = False

    # iterate over current pages and update values
    #def map_current_rows(row):
    for row in current_rows:
        workday_id = row.get_property(teams_properties_dict['workdayID'])
        if not workday_id:
            #return
            continue
        
        if workday_id == 'ROOT':
            uuid = row.id
            data = {
                'notion_uuid': uuid,
                'parent_group': None,
                'Team Name': 'Riot Games',
                'scope': 'riot',
                'Type': 'Company'
            }
            team_obj = rdm_obj(data)
            team_obj.notion_uuid = uuid
            teams_dict['ROOT'] = team_obj
            continue
            #return
        try:
            team_obj = teams_dict[workday_id]
        except:
            if dry_run_obj.dry_run_bool:
                title = row.title_plaintext
                notion_url = BASE_URL + row.id
                property = 'Active'
                change = 'False'
                dry_run_obj.append_entry([title, notion_url, property, change])
            set_active_to_false(row, teams_properties_dict)
            continue
            #return
        team = team_obj.data_transformed
        print(f'updating team {team['Team Name']}')
        if team['Team Name'] == 'REDACTED':
            continue
            #return
        if dry_run_obj.dry_run_bool:
            team_obj.dry_run_change_list = []
        tracking_dict[workday_id] = update_page_properties(
            row,
            TEAMS_COLUMNS_ITERATOR, 
            teams_properties_dict,
            team_obj,
            dry_run_obj.dry_run_bool
        )
        team['notion_uuid'] = team_obj.notion_uuid
    
    #with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        #executor.map(map_current_rows, current_rows)
    
    return tracking_dict

def create_new_teams_pages(view, teams_dict, teams_properties_dict, tracking_dict, dry_run_obj):
    # iterate over every workday id that is false in the tracking dictionary 
    # and create a new page
    #def map_teams_dict(workday_id):
    for workday_id in teams_dict:
        if not tracking_dict[workday_id]:
            team_obj = teams_dict[workday_id]
            team = team_obj.data_transformed
            if team['Team Name'] == 'REDACTED':
                continue
                #return
            print(f'creating team {team['Team Name']}')
            if dry_run_obj.dry_run_bool:
                team_obj.dry_run_change_list = []
            if not dry_run_obj.dry_run_bool:
                new_row = view.collection.add_row()
            else:
                new_row = None
            tracking_dict[workday_id] = set_page_properties(
                new_row,
                TEAMS_COLUMNS_ITERATOR, 
                teams_properties_dict, 
                team_obj,
                dry_run_obj.dry_run_bool
            )
            team['notion_uuid'] = team_obj.notion_uuid

    #with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        #executor.map(map_teams_dict, tracking_dict)

def map_teams_hierarchy(teams_dict, teams_properties_dict, current_rows, dry_run_obj):
    def map_hierarchy(row):
    #for row in current_rows:
        workday_id = row.get_property(teams_properties_dict['workdayID'])
        if not workday_id or workday_id == 'ROOT':
            #continue
            return
        team_obj = teams_dict[workday_id]
        team = team_obj.data_transformed
        parent_team_workday_id = team['parent_group']
        if parent_team_workday_id:
            parent_team_obj = teams_dict[parent_team_workday_id]
            parent_team = parent_team_obj.data_transformed
            if parent_team['Team Name'] == 'REDACTED':
                #continue
                return
            parent_team_notion_uuid = parent_team['notion_uuid']
            current_parent = row.get_property(teams_properties_dict['Parent'])
            try:
                current_parent_notion_uuid = current_parent[0].id
            except:
                current_parent_notion_uuid = None
            if parent_team_notion_uuid == current_parent_notion_uuid:
                #continue
                return
            else:
                if dry_run_obj.dry_run_bool:
                    if not hasattr(team_obj, 'dry_run_change_list'):
                        team_obj.dry_run_change_list = []
                    change_list = ['Parent', parent_team['Team Name']]
                    team_obj.dry_run_change_list.append(change_list)
                    if not hasattr(team_obj, 'title'):
                        team_obj.title = row.title_plaintext
                    return
                print(f'mapping teams: {team['Team Name']} to {parent_team['Team Name']}')
                row.set_property(teams_properties_dict['Parent'], parent_team['notion_uuid'])
        else:
            root_obj = teams_dict['ROOT']
            root = root_obj.data_transformed
            if dry_run_obj.dry_run_bool:
                if not hasattr(team_obj, 'dry_run_change_list'):
                    team_obj.dry_run_change_list = []
                change_list = ['Parent', 'ROOT']
                team_obj.dry_run_change_list.append(change_list)
                if not hasattr(team_obj, 'title'):
                    team_obj.title = row.title_plaintext
                return
            print(f'mapping {team['Full Name']} to ROOT')
            row.set_property(teams_properties_dict['Parent'], root['notion_uuid'])
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(map_hierarchy, current_rows)

def dry_run_append_dict(dry_run_obj, teams_or_products_dict):
    for rdm_rrn in teams_or_products_dict:
        rdm_obj = teams_or_products_dict[rdm_rrn]
        teams_data = rdm_obj.data_transformed
        try:
            title = teams_data['Team Name']
            if not hasattr(rdm_obj, 'notion_uuid'):
                continue
            notion_url = f'{BASE_URL}+{rdm_obj.notion_uuid}'
        except:
            continue
        if hasattr(rdm_obj, 'dry_run_change_list'):
            if rdm_obj.dry_run_change_list:
                for elem in rdm_obj.dry_run_change_list:
                    property = elem[0]
                    change = elem[1]
                    new_row = [title, notion_url, property, change]
                    dry_run_obj.append_entry(new_row)
    title = 'dry run changes'
    write_to_csv(title, dry_run_obj.changes_list)

def update_teams(view, teams_dict, dry_run_obj):
    teams_properties_dict = return_properties(view, TEAMS_COLUMNS, skip=True)
    current_rows = view.collection.get_rows()
    
    #update current pages and create new pages as needed
    tracking_dict = update_current_rows_teams(teams_dict, teams_properties_dict, current_rows, dry_run_obj)
    create_new_teams_pages(view, teams_dict, teams_properties_dict, tracking_dict, dry_run_obj)

    # reinitialize current_rows and teams_properties_dict
    teams_properties_dict = return_properties(view, TEAMS_COLUMNS)
    current_rows = view.collection.get_rows()
    map_teams_hierarchy(teams_dict, teams_properties_dict, current_rows, dry_run_obj)

def update_current_products(products_dict, products_properties_dict, current_rows, dry_run_obj):
    tracking_dict = {}

    for rdm_rrn in products_dict:
        tracking_dict[rdm_rrn] = False

    print(f'Updating existing products')
    #def map_current_rows(row):
    for row in current_rows:
        rdm_rrn = row.get_property(products_properties_dict['rdm_rrn'])
        title = row.title_plaintext
        if not rdm_rrn:
            continue
            #return
        try:
            product_obj = products_dict[rdm_rrn]
        except:
            if dry_run_obj.dry_run_bool:
                title = row.title_plaintext
                notion_url = BASE_URL + row.id
                property = 'Active'
                change = 'False'
                dry_run_obj.append_entry([title, notion_url, property, change])
            set_active_to_false(row, products_properties_dict)
            continue
            #return
        
        product = product_obj.data_transformed
        #print(f'updating product {rdm_rrn}')
        if product['Product Name'] == 'REDACTED':
            continue
            #return
        if dry_run_obj.dry_run_bool:
            product_obj.dry_run_change_list = []
        tracking_dict[rdm_rrn] = update_page_properties(
            row,
            PRODUCTS_COLUMNS_ITERATOR, 
            products_properties_dict,
            product_obj,
            dry_run_obj.dry_run_bool
        )
        product['notion_uuid'] = product_obj.notion_uuid

    #with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        #executor.map(map_current_rows, current_rows)

    return tracking_dict

def create_new_products_pages(view, products_dict, products_properties_dict, tracking_dict, dry_run_obj):
    # iterate over every rdm_rrn that is false in the tracking dictionary 
    # and create a new page
    print(f'Creating new products.')
    #def map_products_dict(rdm_rrn):
    for rdm_rrn in products_dict:
        if not tracking_dict[rdm_rrn]:
            product_obj = products_dict[rdm_rrn]
            product = product_obj.data_transformed
            if product['Product Name'] == 'REDACTED':
                continue
                #return
            print(f'creating product {rdm_rrn}')
            if dry_run_obj.dry_run_bool:
                product_obj.dry_run_change_list = []
            if not dry_run_obj.dry_run_bool:
                new_row = view.collection.add_row()
            else:
                new_row = None
            tracking_dict[rdm_rrn] = set_page_properties(
                new_row,
                PRODUCTS_COLUMNS_ITERATOR, 
                products_properties_dict, 
                product_obj,
                dry_run_obj.dry_run_bool
            )
            product['notion_uuid'] = product_obj.notion_uuid

    #with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        #executor.map(map_products_dict, tracking_dict)

def map_products_hierarchy(products_dict, products_properties_dict, current_rows, teams_dict, dry_run_obj):
    #def map_hierarchy(row):
    for row in current_rows:
        rdm_rrn = row.get_property(products_properties_dict['rdm_rrn'])
        if not rdm_rrn:
            continue
            #return

        product_obj = products_dict[rdm_rrn]
        product = product_obj.data_transformed
        print(f'mapping {product['Product Name']}')
        parent_team_workday_ids = product['owning_group_workday_ids']

        parent_team_notion_uuids = []
        for workday_id in parent_team_workday_ids:
            try:
                team_obj = teams_dict[workday_id]
                team = team_obj.data_transformed
            except:
                continue
            try:
                notion_uuid = team_obj.notion_uuid
                parent_team_notion_uuids.append(notion_uuid)
            except:
                continue
        
        if parent_team_notion_uuids:
            print(f'mapping product {product['Product Name']} to team {team['Team Name']}')
            row.set_property(products_properties_dict['Owning Team(s)'], parent_team_notion_uuids)
        print(f'Finished mapping {product['Product Name']}')
    
    print(f'DONE MAPPPING TEAMS')
    #with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        #executor.map(map_hierarchy, current_rows)

def update_products(view, products_dict, teams_dict, dry_run_obj):
    products_properties_dict = return_properties(view, PRODUCTS_COLUMNS, skip=False)
    current_rows = view.collection.get_rows()

    tracking_dict = update_current_products(products_dict, products_properties_dict, current_rows, dry_run_obj)
    create_new_products_pages(view, products_dict, products_properties_dict, tracking_dict, dry_run_obj)

    # reinitialize current_rows and products_properties_dict
    products_properties_dict = return_properties(view, PRODUCTS_COLUMNS)
    current_rows = view.collection.get_rows()
    map_products_hierarchy(products_dict, products_properties_dict, current_rows, teams_dict, dry_run_obj)

# old code, not sure if needed any more. Almost certainly outdated.
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

def set_owning_property(page, owning_uuid, property_name, properties_dict):
    try:
        page.set_property(properties_dict[property_name], owning_uuid)
    except Exception as e:
        print(f'printing error: {e}')
        if "520 Server Error" in str(e):
            page.set_property(properties_dict[property_name], owning_uuid)
        else:
            raise Exception

def return_notion_uuid(owning_type, team, teams_dict):
    if owning_type == 'Owning Business Unit':
        exclude = ['Studio', 'Pillar', 'Company', 'Business Unit']
    else:
        exclude = ['Studio', 'Pillar', 'Company', 'Business Unit', 'Initiative']

    team_type = team['Type']

    if team_type in exclude:
        return None
    else:
        owning_workday_id = team[owning_type]
        try:
            owning_obj = teams_dict[owning_workday_id]
        except:
            return None
        owning_data = owning_obj.data_transformed
        owning_uuid = owning_data['notion_uuid']

        return owning_uuid

def dry_run_relations(dry_run_obj, row, owning_bu_notion_uuid, owning_initiative_notion_uuid):
    notion_title = row.title_plaintext
    notion_url = row.get_browseable_url()
    if owning_bu_notion_uuid:
        property_changed = 'Owning Business Unit'
        changes_list = [notion_title, notion_url, property_changed, owning_bu_notion_uuid]
        dry_run_obj.append_entry(changes_list)
    if owning_initiative_notion_uuid:
        property_changed = 'Owning Initiative'
        changes_list = [notion_title, notion_url, property_changed, owning_initiative_notion_uuid]
        dry_run_obj.append_entry(changes_list)

def new_update_relations(teams_dict, teams_view, products_dict, products_view, dry_run_obj):
    teams_properties = return_properties(teams_view, TEAMS_COLUMNS)
    products_properties = return_properties(products_view, PRODUCTS_COLUMNS)

    
    bu = 'Business Unit'
    initiative = 'Initiative'
    owning_bu_str = 'Owning Business Unit'
    owning_initiative_str = 'Owning Initiative'
    owning_teams_str = 'Owning Team(s)'

    teams_current_rows = teams_view.collection.get_rows()

    #def map_teams_owners(row):
    for row in teams_current_rows:
        workday_id = row.get_property(teams_properties['workdayID'])
        if not workday_id:
            continue
            #return
        try:
            team_obj = teams_dict[workday_id]
            team = team_obj.data_transformed
        except:
            continue
            #return
        
        owning_bu_notion_uuid = return_notion_uuid(owning_bu_str, team, teams_dict)
        owning_initiative_notion_uuid = return_notion_uuid(owning_initiative_str, team, teams_dict)

        if not owning_bu_notion_uuid and not owning_initiative_notion_uuid:
            continue
            #return

        if dry_run_obj.dry_run_bool:
            dry_run_relations(dry_run_obj, row, owning_bu_notion_uuid, owning_initiative_notion_uuid)
            continue
            #return
        
        if owning_bu_notion_uuid:
            set_owning_property(row, owning_bu_notion_uuid, owning_bu_str, teams_properties)
        if owning_initiative_notion_uuid:
            set_owning_property(row, owning_initiative_notion_uuid, owning_initiative_str, teams_properties)
    
    products_current_rows = products_view.collection.get_rows()

    for row in products_current_rows:
        rdm_rrn = row.get_property(products_properties['rdm_rrn'])
        if not rdm_rrn or rdm_rrn == 'ROOT':
            continue
            #return
        
        try:
            product_obj = products_dict[rdm_rrn]
            product = product_obj.data_transformed
        except:
            continue
            #return
        
        owning_team_workday_ids = product['Owning Group Workday ID']
        owning_bu_notion_uuids = []
        owning_initiative_notion_uuids = []

        for workday_id in owning_team_workday_ids:
            try:
                team_obj = teams_dict[workday_id]
                team = team_obj.data_transformed
            except:
                continue
            
            try:
                owning_team_notion_uuid = team['notion_uuid']
            except:
                print(f'Notion UUID lookup failed for {team['Team Name']}')
                owning_team_notion_uuid = team_obj.associated_page.id
            
            owning_bu_notion_uuid = return_notion_uuid(owning_bu_str, team, teams_dict)
            if owning_bu_notion_uuid:
                owning_bu_notion_uuids.append(owning_bu_notion_uuid)
            
            owning_initiative_notion_uuid = return_notion_uuid(owning_initiative_str, team, teams_dict)
            if owning_initiative_notion_uuid:
                owning_initiative_notion_uuids.append(owning_initiative_notion_uuid)

        if not owning_bu_notion_uuids and not owning_initiative_notion_uuids:
            continue

        if dry_run_obj.dry_run_bool:
            dry_run_relations(dry_run_obj, row, owning_bu_notion_uuids, owning_initiative_notion_uuids)

        if owning_team_notion_uuid:
            set_owning_property(row, owning_team_notion_uuid, owning_teams_str, products_properties)    
        if owning_bu_notion_uuids:
            set_owning_property(row, owning_bu_notion_uuids, owning_bu_str, products_properties)
        if owning_initiative_notion_uuids:
            set_owning_property(row, owning_initiative_notion_uuids, owning_initiative_str, products_properties)

# old update_relations
"""
def update_relations(use_prod_or_dev, dry_run_bool=True):
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
    
    #with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
    #    executor.map(map_rows, teams_rows)
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
    
    for id in teams_dict:
        entry = teams_dict[id]
        if entry['type'] in exlude:
            print(f'excluding {id}. Continuing.')
            continue
        owning_initiative = find_initative(id)
        owning_bu = find_BU(id)
        entry['owning_initiative'] = owning_initiative
        entry['owning_bu'] = owning_bu


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
        print(f'setting values for {id}')
        if owning_initiative:
            row.set_property(teams_properties['Owning Initiative'], owning_initiative)
        if owning_bu:
            row.set_property(teams_properties['Owning Business Unit'], owning_bu)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(map_rows, teams_rows)

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
            print(f'setting values for {owning_team_id}')
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
"""

def update_pages(use_prod_or_dev, dry_run_bool=True):
    #retrive collection ids for prod or dev
    view_dict = use_prod_or_dev_dbs(use_prod_or_dev)
    TEAMS_COLLECTION_VIEW = view_dict['TEAMS_VIEW']
    PRODUCTS_COLLECTION_VIEW = view_dict['PRODUCTS_VIEW']

    dry_run_obj = dry_run(dry_run_bool)

    print('updating teams')
    teams_view = CLIENT.get_collection_view(BASE_URL + TEAMS_COLLECTION_VIEW)
    column_test(teams_view, TEAMS_COLUMNS)

    #todo make this explicitly named to indicate that it's querying RDM
    teams_dict = return_teams_dict()
    update_teams(teams_view, teams_dict, dry_run_obj)

    print('updating products')
    products_view = CLIENT.get_collection_view(BASE_URL + PRODUCTS_COLLECTION_VIEW)
    #verify that breaking changes have not been made to column names
    column_test(products_view, PRODUCTS_COLUMNS)
    products_dict = return_products_dict(teams_dict)
    update_products(products_view, products_dict, teams_dict, dry_run_obj)

    print('updating relations')
    # reinitialize teams and products views
    teams_view = CLIENT.get_collection_view(BASE_URL + TEAMS_COLLECTION_VIEW)
    products_view = CLIENT.get_collection_view(BASE_URL + PRODUCTS_COLLECTION_VIEW)

    file_name = 'teams_and_products.pj'
    files_list = [teams_dict, products_dict]
    pickle_dictionary(files_list, file_name)

    new_update_relations(teams_dict, teams_view, products_dict, products_view, dry_run_obj)

    print('done')

def validate_notion_teams(TEAMS_COLLECTION_VIEW):
    teams_view = CLIENT.get_collection_view(BASE_URL + TEAMS_COLLECTION_VIEW)
    print('starting teams_view')
    teams_view_list = []
    teams_view_headers = ['id', 'title']
    teams_view_header_dict = {}

    for elem in teams_view.collection.get_schema_properties():
        teams_view_headers.append(elem['name'])
        teams_view_header_dict[elem['name']] = elem['slug']

    teams_view_list.append(teams_view_headers)
    print('before get rows')
    current_rows = teams_view.collection.get_rows()

    def map_rows(row):
        print(f'at row {row.title_plaintext}')
        properties = row.get_all_properties()
        values_list = [row.id, row.title_plaintext]
        for key in teams_view_header_dict:
            slug = teams_view_header_dict[key]
            values_list.append(properties[slug])
        teams_view_list.append(values_list)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(map_rows, current_rows)

    print('writing teams_view csv')
    title = 'teams_view'
    write_to_csv(title, teams_view_list)
    
    return teams_view_list

def validate_notion_products(PRODUCTS_COLLECTION_VIEW):
    products_view = CLIENT.get_collection_view(BASE_URL + PRODUCTS_COLLECTION_VIEW)

    print('starting products_view')
    products_view_list = []
    products_view_headers = ['id', 'title']
    products_view_header_dict = {}

    for elem in products_view.collection.get_schema_properties():
        products_view_headers.append(elem['name'])
        products_view_header_dict[elem['name']] = elem['slug']

    products_view_list.append(products_view_headers)
    print('before get rows')
    current_rows = products_view.collection.get_rows()

    def map_rows(row):
        print(f'at row {row.title_plaintext}')
        properties = row.get_all_properties()
        values_list = [row.id, row.title_plaintext]
        for key in products_view_header_dict:
            slug = products_view_header_dict[key]
            values_list.append(properties[slug])
        products_view_list.append(values_list)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(map_rows, current_rows)

    print('writing products_view csv')
    title = 'products_view'
    write_to_csv(title, products_view_list)

def validate_rdm_teams():
    print('starting rdm_teams')
    teams_dict = return_teams_dict()
    rdm_teams_list = []
    rdm_teams_headers = []
    for workday_id in teams_dict:
        teams_obj = teams_dict[workday_id]
        data = teams_obj.data_transformed
        for key in data:
            rdm_teams_headers.append(key)
        break
    
    rdm_teams_list.append(rdm_teams_headers)

    for workday_id in teams_dict:
        teams_obj = teams_dict[workday_id]
        team = teams_obj.data_transformed
        values = []
        for key in rdm_teams_headers:
            values.append(team[key])
        rdm_teams_list.append(values)
    

    print('writing rdm_teams csv')
    title = 'teams_rdm'
    write_to_csv(title, rdm_teams_list)

    return teams_dict

def validate_rdm_products(teams_dict):
    products_dict = return_products_dict(teams_dict)

    rdm_products_list = []
    rdm_products_headers = []
    for rdm_rrn in products_dict:
        products_obj = products_dict[rdm_rrn]
        data = products_obj.data_transformed
        for key in data:
            rdm_products_headers.append(key)
        break
    
    rdm_products_list.append(rdm_products_headers)

    for rdm_rrn in products_dict:
        products_obj = products_dict[rdm_rrn]
        product = products_obj.data_transformed
        values = []
        for key in rdm_products_headers:
            values.append(product[key])
        rdm_products_list.append(values)
    
    print('writing rdm_products csv')
    title = 'products_rdm'
    write_to_csv(title, rdm_products_list)

def validation(use_prod_or_dev):
    view_dict = use_prod_or_dev_dbs(use_prod_or_dev)
    TEAMS_COLLECTION_VIEW = view_dict['TEAMS_VIEW']
    PRODUCTS_COLLECTION_VIEW = view_dict['PRODUCTS_VIEW']

    # generate csv for teams view
    validate_notion_teams(TEAMS_COLLECTION_VIEW)

    # generate csv for rdm groups
    teams_dict = validate_rdm_teams()

    # generate csv for products view
    validate_notion_products(PRODUCTS_COLLECTION_VIEW)

    # generate csv for rdm products
    validate_rdm_products(teams_dict)

def rdm_teams_dict_return():
    response = call_riotorg('rdm_teams')
    data = response['graphql']['group']

    PRODUCTS_OWNING_TEAM_URL_PREFIX = 'https://teams.riotgames.com/teams/'

    return_dict = {}

    for group in data:
        rdm_rrn = group['_rdm_rrn']
        scope = group['scope']
        scope = scope.replace('riot.', '')

        return_dict[rdm_rrn] = scope
    
    return return_dict

def relate_rrns_to_workday_ids(use_prod_or_dev):
    #retrive collection ids for prod or dev
    view_dict = use_prod_or_dev_dbs(use_prod_or_dev)
    TEAMS_COLLECTION_VIEW = view_dict['TEAMS_VIEW']

    teams_dict = return_teams_dict()
    rdm_teams_dict = rdm_teams_dict_return()


    teams_view = CLIENT.get_collection_view(BASE_URL + TEAMS_COLLECTION_VIEW)
    teams_properties_dict = return_properties(teams_view, TEAMS_COLUMNS, skip=True)

    teams_current_rows = teams_view.collection.get_rows()

    match_count = 0
    no_match_count = 0
    war_scope_dict = {}
    war_name_dict = {}

    for workday_id in teams_dict:
        team_obj = teams_dict[workday_id]
        data = team_obj.data_transformed

        war_scope = data['scope']
        war_name = data['Team Name']

        war_scope_dict[war_scope] = team_obj
        war_name_dict[war_name] = team_obj




    rdm_no_match_count = 0
    war_no_match_count = 0

    for row in teams_current_rows:
        rdm_rrn = row.get_property(teams_properties_dict['rdm_rrn'])
        title = row.title_plaintext
        try:
            rdm_scope = rdm_teams_dict[rdm_rrn]
        except:
            print(f'rdm_rrn not found for {title}')
            rdm_no_match_count += 1
            continue

        try:
            team_obj = war_scope_dict[rdm_scope]
            data = team_obj.data_transformed
            workday_id = data['workdayID']
        except:
            print(f'Scope not found in {title}. Trying by name.')
            try:
                team_obj = war_name_dict[title]
                data = team_obj.data_transformed
                workday_id = data['workdayID']
            except:
                print(f'Name not found for {title}.')
                war_no_match_count += 1
                continue
        
        print(f'settinging workdayID: {workday_id} for {title}.')
        row.set_property(teams_properties_dict['workdayID'], workday_id)
    
    print(rdm_no_match_count)
    print(war_no_match_count)

def test(use_prod_or_dev):
    view_dict = use_prod_or_dev_dbs(use_prod_or_dev)
    PRODUCTS_COLLECTION_VIEW = view_dict['PRODUCTS_VIEW']

    products_view = CLIENT.get_collection_view(BASE_URL + PRODUCTS_COLLECTION_VIEW)
    products_properties_dict = return_properties(products_view, PRODUCTS_COLUMNS, skip=False)

    current_rows = products_view.collection.get_rows()

    def set_rows(row):
        value = row.get_property(products_properties_dict['Pager Duty'])
        if value == 'a':
            print(value)
            row.set_property(products_properties_dict['Pager Duty'], '')
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(set_rows, current_rows)

def main():
    use_prod_or_dev = 'DEV'
    dry_run_bool = False
    #relate_rrns_to_workday_ids(use_prod_or_dev)
    update_pages(use_prod_or_dev, dry_run_bool)
    #validation(use_prod_or_dev)

if __name__ == '__main__':
    main()
