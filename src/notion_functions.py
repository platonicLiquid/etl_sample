from datetime import date
import logging
import time

#local imports
from notion_static_attrs import return_views, return_static_column_attrs

#logging
current_date = date.today()
logging.basicConfig(
        filename=f'./logs/notion_functions_log_{current_date}.log',
        encoding='utf-8',
        level=logging.DEBUG
    )

### Notion Functions
def column_test(view, expected_column_names):
    # Test if expected column names are found in given view.
    # If not, raise exception, otherwise, return true.

    columns = view.collection.get_schema_properties()
    
    actual_column_names = set(())
    for elem in columns:
        actual_column_names.add(elem['name'])
    expected_column_names = set(expected_column_names)
    column_test_bool = expected_column_names.issubset(actual_column_names)

    if not column_test_bool:
        print('Incorrect column names.')
        title = view.parent.title
        logging.error(f'Column Names are incorrect for {title}.')
        raise Exception
    else:
        return column_test_bool

def return_properties(view, columns, skip=False):
    properties_dict = {}
    skip_list = return_static_column_attrs()['skip_list']

    for elem in view.collection.get_schema_properties():
        if skip:
            if elem['name'] in skip_list:
                continue
        if elem['name'] in columns:
            properties_dict[elem['name']] = elem['id']
    
    return properties_dict

def use_prod_or_dev_dbs(db_type = 'DEV'):
    collection_view_ids = return_views()

    if db_type == 'DEV':
        VIEW_DICT = {
            'PRODUCTS_VIEW': collection_view_ids['products_view_dev'],
            'TEAMS_VIEW': collection_view_ids['teams_view_dev']
        }
    
    elif db_type == 'PROD':
        VIEW_DICT = {
                'PRODUCTS_VIEW': collection_view_ids['products_view_prod'],
                'TEAMS_VIEW': collection_view_ids['teams_view_prod']
            }
    
    return(VIEW_DICT)

def notion_call_set_page_properties(page, columns_iterator, properties_dictionary, data_obj):
    try:
        data_obj.page_properties_set_bool = False
        for property in columns_iterator:
            try:
                data = data_obj.data_transformed
                notion_property_id = properties_dictionary[property]
                current_property_value = page.get_property(notion_property_id)
                proposed_change = data.get(property, None)
                if not proposed_change:
                    if current_property_value:
                        page.set_property(notion_property_id, [])
                    continue
                if current_property_value == proposed_change:
                    continue
                else:
                    title = page.title_plaintext
                    print(f'Setting {property} for {title}')
                    page.set_property(notion_property_id, proposed_change)
            except Exception as e:
                if "520 Server Error" in str(e) or '500 Server Error' in str(e):
                    try:
                        time.sleep(3)
                        page.set_property(notion_property_id, proposed_change)
                    except:
                        logging.error(f'Server error for {page.get_browsable_url} when setting property: {property}. Error:\n{e}')
                        return
                else:
                    logging.error(f' for notion page id: {page.id}, property: {property}.')
                    e.property = property
                    e.proposed_change = proposed_change
                    raise Exception(e)
        data_obj.page_properties_set_bool = True
        return True
    except Exception as e:
        raise Exception(e)

def strip_uuid(uuid):
    return_uuid = uuid.replace('-', '')
    return return_uuid

def strip_uuids(uuid_list):
    return_list = []
    for uuid in uuid_list:
        stripped_uuid = strip_uuid(uuid)
        return_list.append(stripped_uuid)
    return return_list

def notion_call_set_relations_properties(page, columns_iterator, properties_dictionary, data_obj):
    try:
        data_obj.page_properties_set_bool = False
        for property in columns_iterator:
            try:
                data = data_obj.relations_dict
                notion_property_id = properties_dictionary[property]
                current_property_value = page.get_property(notion_property_id)
                unstriped_change = data.get(property, None)
                if type(unstriped_change) == list:
                    proposed_change = strip_uuids(unstriped_change)
                elif type(unstriped_change) == str:
                    proposed_change = strip_uuid(unstriped_change)
                elif not unstriped_change:
                    proposed_change = None
                else:
                    proposed_change = None
                if not proposed_change:
                    if current_property_value:
                        page.set_property(notion_property_id, [])
                    continue
                if str(current_property_value) == str(proposed_change):
                    continue
                else:
                    title = page.title_plaintext
                    print(f'Setting {property} for {title}')
                    page.set_property(notion_property_id, proposed_change)
            except Exception as e:
                print(e)
                if "520 Server Error" in str(e):
                    page.set_property(notion_property_id, proposed_change)
                if '500 Server Error' in str(e):
                    time.sleep(3)
                    page.set_property(notion_property_id, proposed_change)
                else:
                    logging.error(f' for notion page id: {page.id}, property: {property}.')
                    raise Exception
        data_obj.page_properties_set_bool = True
        return True
    except Exception as e:
        print('Error')
        print(e)
        raise Exception


# The collections block module does not currently 
# support locking or unlocking DBs. This is just 
# "pass" for now.
def lock_unlock(notion_obj, lock_bool):
    pass