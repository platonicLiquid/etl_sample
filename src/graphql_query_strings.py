rdm_products_query_string = """
    query {
        product {
            _rdm_rrn
            name
            type
            status
            description
            product_url
            email
            slack
            pager_duty_url
            groupOwnsProductEdge{
                group{
                    scope
                }
            }
        }
    }
"""

rdm_groups_query_string = """
    query {
        group {
            _rdm_rrn
            group_url
            scope
        }
    }
"""
def return_war_groups_query_str(page):
    if not page:
        page = 1
    
    war_groups_query_string = f"""
        query {{
            allGroups(size: 100, page: {page}) {{
                data {{
                    workdayID
                    scope
                    name
                    groupsID
                    subtype
                    leadershipAssignment {{
                        role
                        description
                        workerMeta {{
                            name
                            username
                        }}
                    }}
                    meta {{
                        missionStatement
                        contact {{
                            slack
                            email
                            support
                        }}
                    }}
                }}
            }}
        }}
    """
    return war_groups_query_string

def return_query_string(products_or_teams, page=None):
    if products_or_teams == 'products' or products_or_teams == 'product':
        return rdm_products_query_string
    elif products_or_teams == 'teams' or products_or_teams == 'team':
        return return_war_groups_query_str(page)
    elif products_or_teams == 'rdm_teams':
        return rdm_groups_query_string
    else:
        raise Exception
