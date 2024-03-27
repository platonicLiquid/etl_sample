from scraper import call_riotorg
from datetime import date
import logging

current_date = date.today()
logging.basicConfig(
        filename=f'./logs/data_extraction_log_{current_date}.log',
        encoding='utf-8',
        level=logging.DEBUG
    )

def extract_teams_data_from_war_groups():
    return_data = []
    end_found = False
    page = 1

    try:
        while not end_found:
            response = call_riotorg('teams', page)
            data = response['graphql']['allGroups']['data']
            if not data:
                end_found = True
                continue
            for elem in data:
                if elem['workdayID'] == None:
                    continue
                else:
                    return_data.append(elem)
            page += 1
    except Exception as e:
        logging.ERROR(e)
        raise Exception

    return return_data

def extract_products_data_from_rdm():
    try:
        return_data = call_riotorg('products')
    except Exception as e:
        logging.ERROR(e)
        raise Exception

    return return_data
