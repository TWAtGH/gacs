
class Replica:
    NEW = 0
    TRANSFER = 1
    COMPLETE = 2
    DELETED = 3

    def __init__(self, rse_obj, file_obj):
        self.id = next(idgen)
        self.rse_obj = rse_obj
        self.file = file_obj
        self.size = 0
        self.state = self.NEW
        self.num_

    def increase(self, amount):
        self.size += amount
        if self.size == self.file.size:
            self.state = self.COMPLETE
        elif self.size > self.file.size:
            self.state = self.COMPLETE
            amount = self.size - self.file.size
            self.size = self.file.size
            #log_warning('increased filesize over the max')
        self.rse_obj.on_replica_increased(self, amount)
