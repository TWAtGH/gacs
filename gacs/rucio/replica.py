
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

    def increase(self, amount):
        amount = min(amount, self.file.size - self.size)
        if amount < 0:
            raise ValueError('Called Replica.increase with negative amount')

        self.size += amount
        if self.size == self.file.size:
            self.state = self.COMPLETE
        self.rse_obj.on_replica_increased(self, amount)

    def delete(self):
        self.state = self.DELETED
        self.rse_obj.on_replica_deleted(self)
