
from gacs.common.utils import next_id

class Replica:
    NEW = 0
    TRANSFER = 1
    COMPLETE = 2
    DELETED = 3

    def __init__(self, rse_obj, file_obj):
        self.id = next_id()
        self.rse_obj = rse_obj
        self.file = file_obj
        self.size = 0
        self.state = self.NEW

    def increase(self, current_time, amount):
        assert amount > 0

        amount = min(amount, self.file.size - self.size)

        self.size += amount
        if self.size == self.file.size:
            self.state = self.COMPLETE
        self.rse_obj.on_replica_increased(self, current_time, amount)

    def delete(self, current_time, remove_from_file=True):
        self.state = self.DELETED
        self.rse_obj.on_replica_deleted(self, current_time)
        if remove_from_file:
            self.file.remove_replica(self)
