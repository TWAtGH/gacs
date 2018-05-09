
from gacs.rucio.replica import Replica

class RucioStorageElement:
    def __init__(self, rse_name):
        self.name = rse_name

        self.used_storage = 0

        self.replica_list = []
        self.replica_by_name = {}

    def create_replica(self, file_obj):
        if file_obj.name in self.replica_by_name:
            pass
        new_replica = Replica(self, file_obj)
        self.replica_list.append(replica_obj)
        self.replica_by_name[file_obj.name] = replica_obj
        file_obj.add_replica(new_replica)
        return new_replica

    def on_replica_increased(self, replica, current_time, amount):
        self.used_storage += amount

    def on_replica_deleted(self, replica, current_time):
        self.used_storage -= replica.size

        self.replica_list.remove(replica)
        del self.replica_by_name[replica.file.name]
