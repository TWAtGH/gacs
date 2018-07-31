
from gacs import abstractions
from gacs.grid import Replica


class Site:
    def __init__(self, name, location_desc):
        self.name = name
        self.location_desc = location_desc

        self.linkselector_by_dst_name = {}
        self.rse_by_name = {}

    def create_linkselector(self, dst_site_obj):
        dst_site_name = dst_site_obj.name
        assert dst_site_name not in self.linkselector_by_dst_name, (self.name, dst_site_name)
        linkselector = abstractions.LinkSelector(self, dst_site_obj)
        self.linkselector_by_dst_name[dst_site_name] = linkselector
        return linkselector

    def create_rse(self, rse_name):
        assert rse_name not in self.rse_by_name, (self.name, rse_name)
        new_rse = StorageElement(self, rse_name)
        self.rse_by_name[rse_name] = new_rse
        return new_rse


class StorageElement:
    def __init__(self, site_obj, name):
        self.site_obj = site_obj
        self.name = name

        self.used_storage = 0

        self.replica_list = []
        self.replica_by_name = {}

    def create_replica(self, file_obj):
        if file_obj.name in self.replica_by_name:
            raise NotImplementedError()
        new_replica = Replica(self, file_obj, len(self.replica_list))
        self.replica_list.append(new_replica)
        self.replica_by_name[file_obj.name] = new_replica
        file_obj.add_replica(new_replica)
        return new_replica

    def increase_replica(self, file_obj, current_time, amount):
        assert amount > 0, amount
        replica_obj = self.replica_by_name[file_obj.name]
        amount = min(amount, file_obj.size - replica_obj.size)
        self.used_storage += amount
        replica_obj.increase(current_time, amount)

    def remove_replica(self, file_obj, current_time):
        replica_obj = self.replica_by_name.pop(file_obj.name)
        tmp = self.replica_list.pop()
        try:
            tmp.rse_index = replica_obj.rse_index
            self.replica_list[tmp.rse_index] = tmp
        except IndexError as err:
            print(err)
            pass
        #self.replica_list.remove(replica_obj)
        self.used_storage -= replica_obj.size
        replica_obj.delete(current_time)
