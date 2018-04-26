
class RucioStorageElement:
    def __init__(self, rse_name):
        self.name = rse_name

        # maybe only store files in RSE as replica
        self.replica_list = []
        self.replica_by_name = {}

        self.deleted_replica_list = []

    def add_replica(self, replica_obj):
        self.replica_list.append(replica_obj)
        self.replica_by_name[replica_obj.file.name] = replica_obj
    
    def on_replica_increased(self, replica, amount):
        pass

    def on_replica_deleted(self, replica):
        self.replica_list.remove(replica)
        del self.replica_by_name[replica.file.name]
        self.deleted_replica_list.append(replica)

    def free_space(self):
        # space_before = self.free_space
        for replica_obj in self.deleted_replica_list:
            # self.free_space += replica_obj.size
            pass
        # return self.free_space - space_before
        return 0
