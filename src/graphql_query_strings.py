product_owners_query_string = """
    query {
        product {
            name
            groupOwnsProductEdge{
                group{
                    _rdm_rrn
                    name
                }
            }
            _rdm_rrn
        }
    }
"""
group_scope_query_string = """
    query {
        group {
            name
            _rdm_rrn
            scope
        }
    }
"""

def return_query_string(products_or_teams):
    if products_or_teams == 'products' or products_or_teams == 'product':
        return product_owners_query_string
    elif products_or_teams == 'teams' or products_or_teams == 'team':
        return group_scope_query_string
    else:
        raise Exception