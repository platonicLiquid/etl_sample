# war groups schema
"""
{
    "data": {
        "allGroups": {
            "data":{
                "workdayID": <workday id>, #prmary key
                "scope": <scope>,
                "name": <name>,
                "groupsID": <groups id>,
                "subtype": <subtype>, #type
                "leadershipAssignment: {
                    "role": <role>,
                    "description": <description>,
                    "workerMeta": {
                        "name": <name>,
                        "username": <username>
                    }
                }
                "meta": {
                    "missionStatement": <mission statement>,
                    "contact": {
                        "slack": <slack>,
                        "email": <email>,
                        "support": <support>
                    }
                }
            }
        }
    }
}
"""
def notion_teams_script_editable_columns():
    NOTION_TEAMS_SCRIPT_EDITABLE_COLUMNS = {
        'Team Name',
        'Type',
        'Mission',
        'Slack',
        'Email',
        'Support Channels'
        'Captain',
        'Parent',
        'Children',
        'Owning Business Unit',
        'Owning Initiative',
        'Products',
        'Riot Org URL (Click to Edit Team)',
        'Active',
        'workdayID',
        'scope'
    }

    return NOTION_TEAMS_SCRIPT_EDITABLE_COLUMNS 

def notion_schema_slug_names():
    NOTION_SCHEMA_SLUG_NAMES = {
        'workdayid',
        'scope',
        'riot_org_url_click_to_edit_team',
        'slack',
        'support_channels'
        'captain',
        'children',
        'type',
        'owning_business_unit',
        'parent',
        'owning_initiative',
        'mission',
        'email',
        'team_name',
    }

    return NOTION_SCHEMA_SLUG_NAMES

def group_data_columns_iterator():
    GROUP_DATA_COLUMNS_ITERATOR = {
        'workdayID',
        'scope',
        'name',
        'groupsID',
        'subtype',
    }

    return GROUP_DATA_COLUMNS_ITERATOR

def data_schema_names_mapped_to_notion_column_names_dict():
    DATA_SCHEMA_NAMES_MAPPED_TO_NOTION_COLUMN_NAMES_DICT = {
        'workdayID':'workdayID',
        'scope': 'scope',
        'name': 'Team Name',
        'subtype': 'Type',
        'missionStatement': 'Mission',
        'slack': 'Slack',
        'email': 'Email',
        'support': 'Support Channels',
        'group_url': 'Riot Org URL (Click to Edit Team)',
        'team_captains': 'Captain'
    }

    return DATA_SCHEMA_NAMES_MAPPED_TO_NOTION_COLUMN_NAMES_DICT