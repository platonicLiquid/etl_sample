# these are the collection view Notion UUIDs. 
# Prod is live production. Dev is for development
#TODO Document how to get these values
PRODUCTS_COLLECTION_VIEW_PROD = '70cf378fbdd741cb9d72540ce61d0094?'\
    'v=aa863dfcef684d4bb0a58f5f193804e7'
TEAMS_COLLECTION_VIEW_PROD = 'f84cbf26f3cd4e3a8adae7fa61dc0206?'\
    'v=cb5473ceba2d473aa140fa7e60a9cca4'
PRODUCTS_COLLECTION_VIEW_DEV = 'c2d332a5ef51458b91adf7ed0ab6cf84?'\
    'v=e3fea6ced3874a778eba09682ae39b0f'
TEAMS_COLLECTION_VIEW_DEV = '4cdb4e0b67584a66b330c058631ff005?'\
    'v=c250c069cc8142e8a93895d9b1c08930'

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
PRODUCTS_COLUMNS_ITERATOR = ['Product Name', 'Type', 'Brief Description',
    'Slack', 'Email', 'Pager Duty', 'Status', 'Riot Org URL (Click to Edit Product)',
    'rdm_rrn']
TEAMS_COLUMNS_ITERATOR = ['Team Name', 'scope', 'Type', 'Mission', 'Slack', 'Email',
    'Riot Org URL (Click to Edit Team)', 'workdayID', 'Active?', 'Support Channels']
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
        'product_column_names': PRODUCTS_COLUMNS,
        'teams_column_names': TEAMS_COLUMNS,
        'products_columns_iterator': PRODUCTS_COLUMNS_ITERATOR,
        'teams_columns_iterator': TEAMS_COLUMNS_ITERATOR,
        'skip_list': SKIP_LIST 
    }

    return return_dict