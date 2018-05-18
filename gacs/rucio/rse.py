
from gacs.rucio.replica import Replica

class RucioStorageElement:
    def __init__(self, rse_name):
        self.name = rse_name

        self.used_storage = 0

        self.replica_list = []
        self.replica_by_name = {}

    def create_replica(self, file_obj):
        if file_obj.name in self.replica_by_name:
            raise NotImplementedError()
        new_replica = Replica(self, file_obj)
        self.replica_list.append(new_replica)
        self.replica_by_name[file_obj.name] = new_replica
        file_obj.add_replica(new_replica)
        return new_replica

    def increase_replica(self, file_obj, current_time, amount):
        assert amount > 0
        replica_obj = self.replica_by_name[file_obj.name]
        amount = min(amount, file_obj.size - replica_obj.size)
        self.used_storage += amount
        replica_obj.increase(current_time, amount)

    def remove_replica(self, file_obj, current_time):
        replica_obj = self.replica_by_name.pop(file_obj.name)
        self.replica_list.remove(replica_obj)
        self.used_storage -= replica_obj.size
        replica_obj.delete(current_time)
