
from gacs.rucio.replica import Replica

class File:
    def __init__(self, file_name, size, die_time):
        self.name = file_name
        self.size = size
        self.die_time = die_time

        self.rse_list = []
        self.rse_by_name = {}
        
        self.replica_list = []
        #self.replica_by_id = {}

    def add_replica(self, replica_obj):
        rse_obj = replica_obj.rse_obj
        self.rse_list.append(rse_obj)
        self.rse_by_name[rse_obj.name] = rse_obj
        self.replica_list.append(replica_obj)

    def delete(self):
        for replica_obj in self.replica_list:
            replica_obj.delete()
        self.rse_list.clear()
        self.rse_by_name.clear()
        self.replica_list.clear()

    def get_complete_replicas(self):
        return [r for r in self.replica_list if r.state == Replica.COMPLETE]

