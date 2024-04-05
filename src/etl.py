from datetime import date, datetime

#lskm imports
from extract_data_from_riotorg import extract_teams_data_from_war_groups, extract_products_data_from_rdm
from transform_data import transform_war_group_data, transform_rdm_product_data
import load_data
from classes import ETLStatus

#dictionary_strings
TEAMS_DATA_RAW = 'teams_data_raw'
PRODUCTS_DATA_RAW = 'products_data_raw'
TEAMS_DICT = 'teams_dict'
PRODUCTS_DICT = 'products_dict'
       

def extract(status_obj):
    # Returns raw data for transformation. Teams is sourced from war-groups;
    # products is sourced from rdm.

    status_obj.time_keeper.stages('extract')

    teams_data_raw = extract_teams_data_from_war_groups()
    products_data_raw = extract_products_data_from_rdm()

    return_dict = {
        TEAMS_DATA_RAW: teams_data_raw,
        PRODUCTS_DATA_RAW: products_data_raw
    }

    status_obj.time_keeper.extract.get_time_elapsed()

    return return_dict

def transform(extracted_data, status_obj):
    # Transforms data into dictionaries for later use. Dictionaries are as below.
    ## Teams:
    ## { workday_id: rdm_obj
    ## }
    ## Products:
    ## { rdm_rrn: rdm_obj
    ## }
    
    status_obj.time_keeper.stages('transform')

    extracted_data: dict

    teams_dict = transform_war_group_data(extracted_data[TEAMS_DATA_RAW])
    products_dict = transform_rdm_product_data(extracted_data[PRODUCTS_DATA_RAW], teams_dict)

    return_dict = {
        TEAMS_DICT: teams_dict,
        PRODUCTS_DICT: products_dict
    }

    status_obj.time_keeper.transform.get_time_elapsed()

    return return_dict

def load(transformed_data, status_obj):
    status_obj.time_keeper.stages('load')

    load_data.load(transformed_data, status_obj)

    status_obj.time_keeper.load.get_time_elapsed()

def main():
    prod_or_dev = 'DEV'
    dry_run_bool = True
    use_concurrency = False
    status_obj = ETLStatus(prod_or_dev, dry_run_bool, use_concurrency)

    print('Extracting data.')
    extracted_data = extract(status_obj)
    print('Transforming data.')
    transformed_data = transform(extracted_data, status_obj)
    print('Beginning loading data.')
    process_time = load(transformed_data, status_obj)
    
    status_obj.time_keeper.get_time_elapsed()

if __name__ == '__main__':
    main()