from datetime import datetime

class data_obj:
    def __init__(self, data_dict, id_type):
        data_dict: dict
        id_type: str
        self.data_transformed = data_dict
        self.id_type = id_type
        self.notion_uuid = None
        self.notion_page_obj = None
        self.page_in_current_rows = False
        self.data_obj_in_source_data = True
        
class dry_run:
    def __init__(self, dry_run_bool):
        dry_run_bool: bool
        self.dry_run_bool = dry_run_bool
        self.changes_list = [['notion title', 'notion url', 'property', 'change']]
        self.set_property_failed_list = []

    def append_entry(self, change_list):
        change_list: list
        self.changes_list.append(change_list)

class TimeKeeper:
    def __init__(self):
        self.start_time = datetime.now()
    
    def get_time_elapsed(self):
        self.end_time = datetime.now()

    def calculate_time_elapsed(self, start_time, end_time):
        # time in:
            #hours \
            #minutes \
            #seconds
        self.time_elapsed = \
            (end_time - start_time).seconds // 3600, \
            (end_time - start_time).seconds % 3600 // 60, \
            (end_time - start_time).seconds % 60

    def stages(self, stage_name):
        if stage_name == 'extract':
            self.extract = TimeKeeper()
        elif stage_name == 'transform':
            self.transform = TimeKeeper()
        elif stage_name == 'load':
            self.load = TimeKeeper()

class ETLStatus:
    def __init__(self, prod_dev, dry_run_bool, use_concurrency_bool):
        self.prod_dev = prod_dev
        self.dry_run_obj = dry_run(dry_run_bool)
        self.use_concurrency = use_concurrency_bool
        self.time_keeper = TimeKeeper()