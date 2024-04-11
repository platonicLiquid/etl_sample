# these are the collection view Notion UUIDs. 
# Prod is live production. Dev is for development
#TODO Document how to get these values
PRODUCTS_COLLECTION_VIEW_PROD = '70cf378fbdd741cb9d72540ce61d0094?'\
    'v=aa863dfcef684d4bb0a58f5f193804e7'
TEAMS_COLLECTION_VIEW_PROD = 'f84cbf26f3cd4e3a8adae7fa61dc0206?'\
    'v=cb5473ceba2d473aa140fa7e60a9cca4'
PRODUCTS_COLLECTION_VIEW_DEV = '8f6d0a7927ab4db7978f943438dd37bc?'\
    'v=fabe8cc11a7b4d1fb9b8778c83753e7f'
TEAMS_COLLECTION_VIEW_DEV = '03b01aaa6c6f4712b14a8fc07af44425?'\
    'v=16730a0eae194494af8f582c9d56ec74'

#Columns are used to map column ids
PRODUCTS_COLUMNS = ['Product Name', 'Type', 'Owning Team(s)', 'Brief Description',
    'Homepage', 'Slack', 'Email', 'Pager Duty', 'Status', 'Riot Org URL (Click to Edit Product)',
    'rdm_rrn', 'Owning Business Unit', 'Owning Initiative']
TEAMS_COLUMNS = ['Team Name', 'Type', 'Mission', 'Active?', 'rdm_rrn', 'scope',
    'Parent', 'Children', 'Captain', 'Slack', 'Email', 'Support Channels',
    'Products', 'Riot Org URL (Click to Edit Team)', 'workdayID', 'Owning Business Unit',
    'Owning Initiative']
#Iterators are used as the properties we want to set.
# todo: add the following properties:
#    'Homepage', 'Captain'
PRODUCTS_COLUMNS_TEXT_ITERATOR = ['Product Name', 'Type', 'Brief Description',
    'Slack', 'Email', 'Pager Duty', 'Status', 'Riot Org URL (Click to Edit Product)',
    'rdm_rrn']
TEAMS_COLUMNS_TEXT_ITERATOR = ['Team Name', 'scope', 'Type', 'Mission', 'Slack', 'Email',
    'Riot Org URL (Click to Edit Team)', 'workdayID', 'Support Channels',
    'Captain']
PRODUCT_COLUMNS_RELATIONS_ITERATOR = ['Owning Team(s)', 'Owning Business Unit', 'Owning Initiative']
TEAMS_COLUMNS_RELATIONS_ITERATOR = ['Parent', 'Owning Business Unit', 'Owning Initiative']
SKIP_LIST = ['Parent', 'Homepage', 'Children', 'Products', 'Owning Team(s)']

def return_views():
    return_dict = {
        'products_view_prod': PRODUCTS_COLLECTION_VIEW_PROD,
        'products_view_dev': PRODUCTS_COLLECTION_VIEW_DEV,
        'teams_view_prod': TEAMS_COLLECTION_VIEW_PROD,
        'teams_view_dev': TEAMS_COLLECTION_VIEW_DEV
    }

    return return_dict

def return_static_column_attrs():
    return_dict = {
        'products_column_names': PRODUCTS_COLUMNS,
        'teams_column_names': TEAMS_COLUMNS,
        'products_columns_text_iterator': PRODUCTS_COLUMNS_TEXT_ITERATOR,
        'teams_columns_text_iterator': TEAMS_COLUMNS_TEXT_ITERATOR,
        'products_columns_relations_iterator': PRODUCT_COLUMNS_RELATIONS_ITERATOR,
        'teams_columns_relations_iterator': TEAMS_COLUMNS_RELATIONS_ITERATOR,
        'skip_list': SKIP_LIST 
    }

    return return_dict