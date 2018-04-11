
class RucioStorageElement:
    def __init__(self, rse_name):
        self.name = rse_name

        # maybe only store files in RSE as replica
        self.replica_list = []
        self.replica_by_name = {}

    def add_replica(self, replica_obj):
        self.replica_list.append(replica_obj)
        self.replica_by_name[replica_obj.file.name] = replica_obj
    
    def on_replica_increased(self, replica, amount):
        pass

