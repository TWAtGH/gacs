
from gacs.rucio.replica import Replica

class File:
    def __init__(self, file_name, size):
        self.name = file_name
        self.size = size

        self.rse_list = []
        self.rse_by_name = {}
        
        self.replica_list = []
        #self.replica_by_id = {}

    def add_replica(self, replica_obj):
        rse_obj = replica_obj.rse_obj
        self.rse_list.append(rse_obj)
        self.rse_by_name[rse_obj.name] = rse_obj
        self.replica_list.append(replica_obj)

    def get_complete_replicas(self):
        return [r for r in self.replica_list if r.state == Replica.COMPLETE]

