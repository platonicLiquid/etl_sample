email = 'your_email@riotgames.com'
token = 'some_token'
# get this token from the session cookies by inspecting a notion page when logged in
# using a riot account
session_token = 'some_v2_token'

def secrets():
    secrets = {
        'email': email,
        'token': token,
        'session_token': session_token
    }
    return(secrets)