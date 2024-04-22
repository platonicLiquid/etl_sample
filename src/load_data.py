#native imports
import concurrent.futures
from datetime import date
import time
import logging

#3rd party modules
from notion.client import NotionClient

#local imports
from notion_secrets import secrets
import etl_classes
import notion_functions
from notion_static_attrs import return_static_column_attrs

#logging
current_date = date.today()
logging.basicConfig(
        filename=f'./logs/load_log_{current_date}.log',
        encoding='utf-8',
        level=logging.ERROR
    )

#concurrency setup
NUM_THREADS = 16

# Client setup for Notion module. Uses notion secrets file.
# Base URL is for our notion instance.
CLIENT = NotionClient(token_v2= secrets()['session_token'])
BASE_URL = 'https://www.notion.so/riotgames/'

# globals
type_dict = {
    'workdayID': 'team',
    'rdm_rrn': 'product'
}
owning_initative_str = 'Owning Initiative'
owning_bu_str = 'Owning Business Unit'
owning_teams_str = 'Owning Team(s)'

# Load Data:
def set_uuid_and_page_obj(row, data_obj):
    data_obj.page_in_current_rows = True
    uuid = row.id
    data_obj.notion_uuid = uuid
    data_obj.notion_page_obj = row

def set_root_page(row, data_dict):
    uuid = row.id
    data = {
        'notion_uuid': uuid,
        'parent_group': None,
        'Team Name': 'Riot Games',
        'scope': 'riot',
        'Type': 'Company',
        'Active?': 'Active'
    }
    data_obj = etl_classes.data_obj(data, 'workdayID')
    data_obj.page_in_current_rows = True
    set_uuid_and_page_obj(row, data_obj)
    data_dict['ROOT'] = data_obj

def stage_for_setting_active_to_false(row, data_dict, id_type, id):
    data = { 'Active?': 'Inactive'}
    data_obj = etl_classes.data_obj(data, id_type)
    data_obj.data_obj_in_source_data = False
    set_uuid_and_page_obj(row, data_obj)
    data_dict[id] = data_obj

def process_rows(row, data_dict, columns_properties_dict, id_type):
    title = row.title_plaintext
    print(f'Processing {id_type}: {title}')

    id = row.get_property(columns_properties_dict[id_type])
    
    #TODO these pages need to be marked as need to be deleted in Notion.
    if not id:
        return
    
    if id == 'ROOT':
        set_root_page(row, data_dict)
        return

    try:
        data_obj = data_dict[id]
        set_uuid_and_page_obj(row, data_obj)
    except:
        if not id:
            #flag for deletion
            return
        else:
            stage_for_setting_active_to_false(row, data_dict, id_type, id)

def match_data_to_current_rows(data_dict, current_rows, columns_properties_dict, status_obj):
    for id in data_dict:
        data_obj = data_dict[id]
        
    id_type = data_obj.id_type

    if status_obj.use_concurrency:
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            future_to_rows = {
                executor.submit(
                    process_rows,
                    row,
                    data_dict,
                    columns_properties_dict,
                    id_type
                ): row for row in current_rows
            }
            for future in concurrent.futures.as_completed(future_to_rows):
                row = future_to_rows[future]
                try:
                    future.result()
                except Exception as e:
                    browseable_url = data_obj.notion_page_obj.get_browseable_url()
                    page_title = data_obj.notion_page_obj.title_plaintext
                    data_obj_name = data_obj.name
                    logging.error(
                        f'PROPERTY SETTING ERROR for {data_obj_name} at Notion page: {browseable_url}.\n'
                        f'Page title: "{page_title}".\n'\
                        f'Property: {e.property}\n'\
                        f'Proposed Change: {e.proposed_change}'
                    )
    else:
        for row in current_rows:
            process_rows(
                row,
                data_dict,
                columns_properties_dict,
                id_type
            )

    return 

def stage_changes_for_current_rows(data_dict, view, columns_properties_dict, status_obj):
    current_rows = view.collection.get_rows()
    match_data_to_current_rows(data_dict, current_rows, columns_properties_dict, status_obj)

def stage_changes(data, client_objects, properties_dicts, status_obj):
    #unpack variables
    teams_dict = data['teams_dict']
    products_dict = data['products_dict']

    teams_view = client_objects['teams_view']
    products_view = client_objects['products_view']

    teams_properties_dict = properties_dicts['teams_properties_dict']
    products_properties_dict = properties_dicts['products_properties_dict']

    #stage changes
    print('Staging changes for teams.')
    stage_changes_for_current_rows(teams_dict, teams_view, teams_properties_dict, status_obj)
    print('Staging changes for products.')
    stage_changes_for_current_rows(products_dict, products_view, products_properties_dict, status_obj)

def execute_generate_new_pages(data_obj, view):
    if data_obj.notion_uuid:
        return
    try:
        new_row = view.collection.add_row()
    except Exception as e:
        if str(413) in str(e):
            time.sleep(3)
            new_row = view.collection.add_row()
        else:
            logging.error(f'ROW CREATION ERROR: Unable to create new row for {data_obj.name}. Error:\n{e}')
    set_uuid_and_page_obj(new_row, data_obj)

def generate_new_pages(data_dict, view, status_obj):
    if status_obj.use_concurrency:
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            future_to_data_obj = {
                executor.submit(
                    execute_generate_new_pages,
                    data_obj,
                    view
                ): data_obj for data_obj in data_dict.values()
            }
            for future in concurrent.futures.as_completed(future_to_data_obj):
                data_obj = future_to_data_obj[future]
                try:
                    future.result()
                except Exception as e:
                    browseable_url = data_obj.notion_page_obj.get_browseable_url()
                    page_title = data_obj.notion_page_obj.title_plaintext
                    data_obj_name = data_obj.name
                    logging.error(f'PAGE GENERATION ERROR for: {data_obj_name} at Notion page: {browseable_url} page title: "{page_title}".')
    else:
        for data_obj in data_dict.values():
            execute_generate_new_pages(data_obj, view)

def generate_new_pages_setup_then_execute(data, client_objects, status_obj):
    teams_dict = data['teams_dict']
    products_dict = data['products_dict']

    teams_view = client_objects['teams_view']
    products_view = client_objects['products_view']

    print('Creating pages for teams.')
    generate_new_pages(teams_dict, teams_view, status_obj)
    print('Creating pages for Products.')
    generate_new_pages(products_dict, products_view, status_obj)

def set_text_properties(page, data_obj, properties_dict, id_type):
    if id_type == 'workdayID':
        columns_iterator = return_static_column_attrs()['teams_columns_text_iterator']
    elif id_type == 'rdm_rrn':
        columns_iterator = return_static_column_attrs()['products_columns_text_iterator']
    else:
        try:
            raise ValueError(f'Invalid idtype: {id_type}')
        except ValueError:
            logging.error(ValueError)
        
    notion_functions.notion_call_set_page_properties(page, columns_iterator, properties_dict, data_obj)

def active_test(data):
    if data['Active?'] == 'Inactive' or data['Active?'] == 'False':
        return True
    else:
        return False

def map_teams_relations(data_obj, teams_dict):
    relations_dict = {}

    data = data_obj.data_transformed

    if active_test(data):
        return
    
    parent_id = data['parent_group']
    owning_bu_id = data[owning_bu_str]
    owning_initiative_id = data[owning_initative_str]

    if parent_id:
        parent_data_obj = teams_dict[parent_id]
        parent_data = parent_data_obj.data_transformed
        if active_test(parent_data):
            parent_notion_uuid = None
        else:
            parent_notion_uuid = parent_data_obj.notion_uuid
        if not parent_notion_uuid:
            relations_dict['Parent'] = None
        else:
            relations_dict['Parent'] = parent_notion_uuid
    
    if owning_bu_id:
        owning_bu_obj = teams_dict[owning_bu_id]
        owning_bu_data = owning_bu_obj.data_transformed
        if active_test(owning_bu_data):
            owning_bu_uuid = None
        else:
            owning_bu_uuid = owning_bu_obj.notion_uuid
        if not owning_bu_uuid:
            relations_dict[owning_bu_str] = None
        else:
            relations_dict[owning_bu_str] = owning_bu_uuid
    
    if owning_initiative_id:
        owning_initiative_obj = teams_dict[owning_initiative_id]
        owning_initiative_data = owning_initiative_obj.data_transformed
        if active_test(owning_initiative_data):
            owning_initiative_uuid = None
        else:
            owning_initiative_uuid = owning_initiative_obj.notion_uuid
        if not owning_initiative_uuid:
            relations_dict[owning_initative_str] = None
        else:
            relations_dict[owning_initative_str] = owning_initiative_uuid
    
    data_obj.relations_dict = relations_dict

def map_products_relations(data_obj, teams_dict):
    data = data_obj.data_transformed

    if active_test(data):
        return
    
    parent_ids = data.get('Owning Group Workday ID', None)

    if not parent_ids:
        relations_dict = {}
        relations_dict[owning_teams_str] = None
        relations_dict[owning_bu_str] = None
        relations_dict[owning_initative_str] = None
        data_obj.relations_dict = relations_dict
        return
    
    owning_teams_uuids = []
    owning_bu_uuids = []
    owning_initiative_uuids = []

    for id in parent_ids:
        try:
            owning_team_obj = teams_dict[id]
            owning_team_uuid = owning_team_obj.notion_uuid
        except:
            continue
        owning_teams_uuids.append(owning_team_uuid)

        owning_team_data = owning_team_obj.data_transformed
        if active_test(owning_team_data):
            owning_bu_id = None
            owning_initiative_id = None
            return
        
        else:
            owning_bu_id = owning_team_data[owning_bu_str]
            owning_initiative_id = owning_team_data[owning_initative_str]
    
        if owning_bu_id:
            owning_bu_obj = teams_dict[owning_bu_id]
            owning_bu_uuid = owning_bu_obj.notion_uuid
            if owning_bu_uuid:
                owning_bu_uuids.append(owning_bu_uuid)

        if owning_initiative_id:
            owning_initiative_obj = teams_dict[owning_initiative_id]
            owning_initiative_uuid = owning_initiative_obj.notion_uuid
            if owning_initiative_uuid:
                owning_initiative_uuids.append(owning_initiative_uuid)

    relations_dict = {
        owning_teams_str: owning_teams_uuids,
        owning_bu_str: owning_bu_uuids,
        owning_initative_str: owning_initiative_uuids
    }

    data_obj.relations_dict = relations_dict

def set_relations_properties(page, data_obj, properties_dict, teams_dict, id_type):
    if id_type == 'workdayID':
        columns_iterator = return_static_column_attrs()['teams_columns_relations_iterator']
        map_teams_relations(data_obj, teams_dict)
    elif id_type == 'rdm_rrn':
        columns_iterator = return_static_column_attrs()['products_columns_relations_iterator']
        map_products_relations(data_obj, teams_dict)
    else:
        try:
            raise ValueError(f'Invalid idtype: {id_type}')
        except ValueError:
            logging.error(ValueError)
    
    notion_functions.notion_call_set_relations_properties(page, columns_iterator, properties_dict, data_obj)

def execute_update_all_pages(data_obj, properties_dict, teams_dict):
    # If there is .notion_page_obj = None, return.
    # If data['Active?'] = 'Inactive', return.
    # Else, set text properties and then set relations properties.

    if not data_obj.notion_page_obj:
        return
    data = data_obj.data_transformed
    if active_test(data):
        return
    
    page = data_obj.notion_page_obj
    id_type = data_obj.id_type
    set_text_properties(page, data_obj, properties_dict, id_type)
    set_relations_properties(page, data_obj, properties_dict, teams_dict, id_type)

def update_all_pages(data_dict, properties_dict, teams_dict, status_obj):
    if status_obj.use_concurrency:
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            future_to_data_obj = {
                executor.submit(
                    execute_update_all_pages,
                    data_obj,
                    properties_dict,
                    teams_dict
                ): data_obj for data_obj in data_dict.values()
            }
            for future in concurrent.futures.as_completed(future_to_data_obj):
                data_obj = future_to_data_obj[future]
                try:
                    future.result()
                except Exception as e:
                    browseable_url = data_obj.notion_page_obj.get_browseable_url()
                    page_title = data_obj.notion_page_obj.title_plaintext
                    data_obj_name = data_obj.name
                    logging.error(
                        f'PROPERTY SETTING ERROR for {data_obj_name} at Notion page: {browseable_url}.\n'
                        f'Page title: "{page_title}".\n'\
                        f'Property: {e.property}\n'\
                        f'Proposed Change: {e.proposed_change}'
                    )
    else:
        for data_obj in data_dict.values():
            execute_update_all_pages(data_obj, properties_dict, teams_dict)

def update_all_pages_setup_then_execute(data, properties_dicts, status_obj):
    teams_dict = data['teams_dict']
    products_dict = data['products_dict']

    teams_properties_dict = properties_dicts['teams_properties_dict']
    products_properties_dict = properties_dicts['products_properties_dict']

    print('Updating Teams.')
    update_all_pages(teams_dict, teams_properties_dict, teams_dict, status_obj)
    print('Updating Products.')
    update_all_pages(products_dict, products_properties_dict, teams_dict, status_obj)

def execute_changes(data, client_objects, properties_dicts, status_obj):
    generate_new_pages_setup_then_execute(data, client_objects, status_obj)
    update_all_pages_setup_then_execute(data, properties_dicts, status_obj)

def return_client_objects(prod_or_dev):
    view_ids_dict = notion_functions.use_prod_or_dev_dbs(prod_or_dev)
    teams_view_id = view_ids_dict['TEAMS_VIEW']
    products_view_id = view_ids_dict['PRODUCTS_VIEW']

    teams_view = CLIENT.get_collection_view(BASE_URL + teams_view_id)
    products_view = CLIENT.get_collection_view(BASE_URL + products_view_id)

    return_dict = {
        'teams_view': teams_view,
        'products_view': products_view
    }

    return return_dict

def return_properties_dictionaries(client_objects):
    return_dict = {}

    for client_name in client_objects:
        if 'team' in client_name:
            view = client_objects[client_name]
            teams_properties_dict = notion_functions.return_properties(
                view,
                return_static_column_attrs()['teams_column_names']
            )
            return_dict['teams_properties_dict'] = teams_properties_dict
        if 'product' in client_name:
            view = client_objects[client_name]
            products_properties_dict = notion_functions.return_properties(
                view,
                return_static_column_attrs()['products_column_names']
            )
            return_dict['products_properties_dict'] = products_properties_dict
    
    return return_dict

def load(data, status_obj):
    prod_or_dev = status_obj.prod_dev
    client_objects = return_client_objects(prod_or_dev)
    properties_dicts = return_properties_dictionaries(client_objects)

    stage_changes(data, client_objects, properties_dicts, status_obj)
    execute_changes(data, client_objects, properties_dicts, status_obj)