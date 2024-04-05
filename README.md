# Setup

* Install the RDM-API-Wrapper by running this command in your terminal: `pip install git+https://gh.riotgames.com/matthew-yocum/riotorg-apis-wrapper#egg=riotorg-apis-wrapper`.
  * If you ever need to upgrade the wrapper run this command instead: `pip install --upgrade git+https://gh.riotgames.com/matthew-yocum/riotorg-apis-wrapper#egg=riotorg-apis-wrapper`
* Get your v2 token from notion by inspecting in the browser and looking under "Application" -> "Cookies". Replace the v2 token value in ["/src/notion_secrets_edit.py](./src/notion_secrets_edit.py) and save file as "notion_secrets.py".
* Get a RDM API key and replace the token value in ["/src/rdm_secrets_edit.py"](./src/rdm_secrets_edit.py). Save file as "rdm_secrets.py".
* Retrieve a WAR-Groups API key are relace the token value in ["/src/war_groups_secrects_edit.py"](./src/war_groups_secrets_edit.py). Save file as "war_groups_secrets.py".

# Running

Main file is ["etl.py"](./src/etl.py). Steps to run:

1. Verify that you have your API keys and Notion secret installed correctly.
2. Verify that everything is setup correctly (it should be unless you change it).
   * The function `main()` should have various parameters set and then call the separate portions of ETL.
3. Run the program and verify the results at [here](https://www.notion.so/riotgames/Databases-1f60779bd115482c9aa8468f99830f18?pvs=4).
   *  Prod DBs are currently commented out so we don't accidently screw up the live data.