class rdm_obj:
    def __init__(self, data_dict, id_type):
        data_dict: dict
        id_type: str
        self.data_transformed = data_dict
        self.id_type = id_type
        
class dry_run:
    def __init__(self, dry_run_bool):
        dry_run_bool: bool
        self.dry_run_bool = dry_run_bool
        self.changes_list = [['notion title', 'notion url', 'property', 'change']]
        self.set_property_failed_list = []

    def append_entry(self, change_list):
        change_list: list
        self.changes_list.append(change_list)