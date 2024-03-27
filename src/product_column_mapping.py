def data_schema_names_mapped_to_notion_column_names_dict():
    DATA_SCHEMA_NAMES_MAPPED_TO_NOTION_COLUMN_NAMES_DICT = {
        'rdm_rrn': 'rdm_rrn',
        'name': 'Product Name',
        'type': 'Type',
        'status': 'Status',
        'description': 'Brief Description',
        'product_url': 'product_url',
        'riot_org_url': 'Riot Org URL (Click to Edit Product)',
        'slack': 'Slack',
        'email': 'Email',
        'pager_duty_url': 'Pager Duty',
        'owning_group_workday_ids': 'Owning Group Workday ID'
    }

    return DATA_SCHEMA_NAMES_MAPPED_TO_NOTION_COLUMN_NAMES_DICT

def notion_schema_slug_names():
    NOTION_SCHEMA_SLUG_NAMES = {
        'product_name',
        'type',
        'homepage',
        'brief_description',
        'riot_org_url_click_to_edit_product',
        'slack',
        'email',
        'pager_duty',
        'status',
        'rdm_rrn'
    }

    return notion_schema_slug_names

def products_data_columns_iterator():
    PRODUCTS_DATA_COLUMNS_ITERATOR = {
        '_rdm_rrn',
        'name',
        'type',
        'status',
        'description',
        'product_url',
        'email',
        'slack',
        'pager_duty_url'
    }

    return PRODUCTS_DATA_COLUMNS_ITERATOR