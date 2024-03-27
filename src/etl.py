from datetime import date, datetime

#lskm imports
from extract_data_from_riotorg import extract_teams_data_from_war_groups, extract_products_data_from_rdm
from transform_data import transform_war_group_data, transform_rdm_product_data
import load_data
from rdm_classes import dry_run

#dictionary_strings
TEAMS_DATA_RAW = 'teams_data_raw'
PRODUCTS_DATA_RAW = 'products_data_raw'
TEAMS_DICT = 'teams_dict'
PRODUCTS_DICT = 'products_dict'


def extract():
    # Returns raw data for transformation. Teams is sourced from war-groups;
    # products is sourced from rdm.

    teams_data_raw = extract_teams_data_from_war_groups()
    products_data_raw = extract_products_data_from_rdm()

    return_dict = {
        TEAMS_DATA_RAW: teams_data_raw,
        PRODUCTS_DATA_RAW: products_data_raw
    }

    return return_dict

def transform(extracted_data):
    # Transforms data into dictionaries for later use. Dictionaries are as below.
    ## Teams:
    ## { workday_id: rdm_obj
    ## }
    ## Products:
    ## { rdm_rrn: rdm_obj
    ## }
    
    extracted_data: dict

    teams_dict = transform_war_group_data(extracted_data[TEAMS_DATA_RAW])
    products_dict = transform_rdm_product_data(extracted_data[PRODUCTS_DATA_RAW], teams_dict)

    return_dict = {
        TEAMS_DICT: teams_dict,
        PRODUCTS_DICT: products_dict
    }

    return return_dict

def load(transformed_data, prod_or_dev, dry_run_obj):
    load_data.load(transformed_data, prod_or_dev, dry_run_obj)

def main():
    start_time = datetime.now()
    prod_or_dev = 'DEV'
    dry_run_obj = dry_run(True)

    extracted_data = extract()
    transformed_data = transform(extracted_data)
    process_time = load(transformed_data, prod_or_dev, dry_run_obj)
    end_time = datetime.now()
    elapsed_time = end_time - start_time / 60
    print(process_time)
    print(elapsed_time)

if __name__ == '__main__':
    main()