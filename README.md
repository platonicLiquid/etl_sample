# Setup

* Install the RDM-API-Wrapper by running this command in your terminal: `pip install git+https://gh.riotgames.com/matthew-yocum/rdm-api-wrapper#egg=rdm-api-wrapper`.
  * If you ever need to upgrade the wrapper run this command instead: `pip install --upgrade git+https://gh.riotgames.com/matthew-yocum/rdm-api-wrapper#egg=rdm-api-wrapper`
* Get your v2 token from notion by inspecting in the browser and looking under "Application" -> "Cookies". Replace the v2 token value in ["/src/notion_secrets_edit.py](./src/notion_secrets_edit.py) and save file as "notion_secrets.py".
* Get a RDM API key and replace the token value in ["/src/rdm_secrets_edit.py"](./src/rdm_secrets_edit.py). Save file as "rdm_secrets.py".