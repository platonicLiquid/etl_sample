import csv

import pandas

from etl_classes import ETLStatus
from etl import extract

status_obj = ETLStatus('DEV', False, True)
raw_data = extract(status_obj)

teams_raw_data = raw_data['teams_data_raw']

teams_header = ['name', 'workdayID', 'scope', 'Type',
    'Captain', 'Mission', 'Slack', 'email', 'support']
teams_list = [teams_header]

for elem in teams_raw_data:
    workdayID = elem['workdayID']
    scope = elem['scope']
    name = elem['name']
    subtype = elem['subtype']
    
    captain = []
    for person in elem['leadershipAssignment']:
        if person['description'] == 'team captain':
            workerMeta = person['workerMeta']
            captain.append(workerMeta['name'])
    if not captain:
        captain = ''
    else:
        new_captain = ''
        for captain_name in captain:
            new_captain = new_captain + ' ' + captain_name
        captain = new_captain

    meta = elem['meta']
    missionStatement = meta['missionStatement']
    contact = meta['contact']
    slack = contact['slack']
    if not slack:
        slack = ''
    email = contact['email']
    if not email:
        email = ''
    support = contact['support']
    if not support:
        support = ''

    new_line = [
        name, workdayID, scope, subtype,
        captain, missionStatement, slack, email, support
    ]
    teams_list.append(new_line)


products_raw_data = raw_data['products_data_raw']['graphql']['product']

products_header = [
    'name', 'rdm_rrn', 'type', 'status',
    'description', 'email', 'slack', 'pager_duty', 'owning teams'
]
products_list = [products_header]

for elem in products_raw_data:
    rdm_rrn = elem['_rdm_rrn']
    name = elem['name']
    subtype = elem['type']
    status = elem['status']
    description = elem['description']
    email = elem['email']
    slack = elem['slack']
    pager_duty_url = elem['pager_duty_url']

    owning_team_scope = []
    for group in elem['groupOwnsProductEdge']:
        scope = group['group']['scope']
        owning_team_scope.append(scope)
    if not owning_team_scope:
        owning_team_scope = ''

    new_line = [
        name, rdm_rrn, subtype, status, description,
        email, slack, pager_duty_url, owning_team_scope
    ]
    products_list.append(new_line)

df_teams_list = pandas.DataFrame(teams_list)
df_products_list = pandas.DataFrame(products_list)

df_teams_list.to_csv('./validation_files/teams_raw_output.csv', index=False)
df_products_list.to_csv('./validation_files/products_raw_output.csv', index=False)
