
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
        self.size += amount
        assert self.size <= self.file.size, (self.state, selft.size, self.file.size)
        if self.size == self.file.size:
            self.state = self.AVAILABLE

    def delete(self, current_time):
        self.size = 0
        self.state = self.DELETED
