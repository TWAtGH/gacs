
from gacs.common import monitoring, utils


class File:
    def __init__(self, file_name, size, die_time, file_index):
        self.name = file_name
        self.size = size
        self.die_time = die_time
        self.file_index = file_index

        self.rse_list = []
        self.rse_by_name = {}

        self.replica_list = []
        #self.replica_by_id = {}

        self.transfer_list = []

    def add_transfer(self, transfer):
        self.transfer_list.append(transfer)

    def remove_transfer(self, transfer):
        self.transfer_list.remove(transfer)

    def add_replica(self, replica_obj):
        rse_obj = replica_obj.rse_obj
        self.rse_list.append(rse_obj)
        self.rse_by_name[rse_obj.name] = rse_obj
        self.replica_list.append(replica_obj)

    def remove_replica(self, replica_obj):
        raise NotImplemented()
        rse_obj = replica_obj.rse_obj
        self.rse_list.remove(rse_obj)
        del self.rse_by_name[rse_obj.name]
        self.replica_list.remove(replica_obj)

    def delete(self, current_time):
        monitoring.OnFileDeletion(self)
        for transfer in self.transfer_list:
            transfer.delete()
        for rse in self.rse_list:
            rse.remove_replica(self, current_time)
        self.rse_list.clear()
        self.rse_by_name.clear()
        self.replica_list.clear()
        self.transfer_list.clear()


class Replica:
    CORRUPTED = 0
    AVAILABLE = 1
    DELETED = 2

    def __init__(self, rse_obj, file_obj, rse_index):
        self.id = utils.next_id()
        self.rse_obj = rse_obj
        self.file = file_obj
        self.rse_index = rse_index
        self.size = 0
        self.state = self.CORRUPTED

    def increase(self, current_time, amount):
        self.size += amount
        assert self.size <= self.file.size, (self.state, selft.size, self.file.size)
        if self.size == self.file.size:
            self.state = self.AVAILABLE

    def delete(self, current_time):
        self.size = 0
        self.state = self.DELETED
