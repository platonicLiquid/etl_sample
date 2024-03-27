from datetime import date, time
import logging

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
