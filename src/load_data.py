#native imports
import concurrent.futures
from datetime import date, datetime
import logging

#3rd party modules
from notion.client import NotionClient

#local imports
from notion_secrets import secrets
from rdm_classes import dry_run, rdm_obj
import notion_functions
from notion_static_attrs import return_static_column_attrs

#logging
current_date = date.today()
logging.basicConfig(
        filename=f'./logs/load_log_{current_date}.log',
        encoding='utf-8',
        level=logging.DEBUG
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
        'Type': 'Company'
    }
    data_obj = rdm_obj(data, 'workdayID')
    set_uuid_and_page_obj(row, data_obj)
    data_dict['ROOT'] = data_obj

def stage_for_setting_active_to_false(row, data_dict, id_type):
    data = { 'Active?: False'}
    data_obj = rdm_obj(data, id_type)
    set_uuid_and_page_obj(row, data_obj)
    data_dict[id] = data_obj

def process_rows(row, data_dict, columns_properties_dict, id_type, processed_time):
    start_time = datetime.now()
    id = row.get_property(columns_properties_dict[id_type])
    
    #TODO these pages need to be marked as need to be deleted in Notion.
    if not id:
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        processed_time += elapsed_time
        return
    
    if id == 'ROOT':
        set_root_page(row, data_dict)
        return

    try:
        data_obj = data_dict[id]
        set_uuid_and_page_obj(row, data_obj)
    except:
        stage_for_setting_active_to_false(row, data_dict, id_type)
        return

def match_data_to_current_rows(data_dict, current_rows, columns_properties_dict, total_process_time):
    for id in data_dict:
        data_obj = data_dict[id]
        data_obj.page_in_current_rows = False

    id_type = data_obj.id_type

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        future_to_rows = {
            executor.submit(
                process_rows,
                row,
                data_dict,
                columns_properties_dict,
                id_type,
                total_process_time
            ): row for row in current_rows
        }
        for future in concurrent.futures.as_completed(future_to_rows):
            row = future_to_rows[future]
            try:
                future.result()
            except Exception as e:
                print('%r generated an exception: %s' % (row, e))
                raise Exception
            
    return 


def stage_changes_for_current_rows(data_dict, view, columns_properties_dict, total_process_time):
    current_rows = view.collection.get_rows()
    match_data_to_current_rows(data_dict, current_rows, columns_properties_dict, total_process_time)

def stage_changes(data, client_objects, dry_run_obj, total_process_time):
    #unpack variables
    teams_dict = data['teams_dict']
    products_dict = data['products_dict']

    teams_view = client_objects['teams_view']
    products_view = client_objects['products_view']

    teams_properties_dict = notion_functions.return_properties(
            teams_view,
            return_static_column_attrs()['teams_column_names']
        )
    products_properties_dict = notion_functions.return_properties(
            products_view,
            return_static_column_attrs()['product_column_names']
        )

    #stage changes
    stage_changes_for_current_rows(teams_dict, teams_view, teams_properties_dict, total_process_time)
    stage_changes_for_current_rows(products_dict, products_view, products_properties_dict, dry_run_obj)
    print(total_process_time/60)

def execute_changes(data):
    pass

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

def load(data, prod_or_dev='DEV', dry_run_obj=dry_run(True)):
    total_process_time = 0

    client_objects = return_client_objects(prod_or_dev)

    stage_changes(data, client_objects, dry_run_obj, total_process_time)
    execute_changes(data)