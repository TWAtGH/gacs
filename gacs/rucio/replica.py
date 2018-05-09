
from gacs.common.utils import next_id

class Replica:
    CORRUPTED = 0
    AVAILABLE = 1
    DELETED = 2

    def __init__(self, rse_obj, file_obj):
        self.id = next_id()
        self.rse_obj = rse_obj
        self.file = file_obj
        self.size = 0
        self.state = self.CORRUPTED

    def increase(self, current_time, amount):
        assert amount > 0

        amount = min(amount, self.file.size - self.size)

        self.size += amount
        if self.size == self.file.size:
            self.state = self.AVAILABLE
        self.rse_obj.on_replica_increased(self, current_time, amount)

    def delete(self, current_time, remove_from_file=True):
        self.rse_obj.on_replica_deleted(self, current_time)
        if remove_from_file:
            self.file.remove_replica(self)
        self.size = 0
        self.state = self.DELETED
