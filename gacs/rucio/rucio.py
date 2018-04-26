
from gacs.rucio.file import File
from gacs.rucio.replica import Replica
from gacs.rucio.rse import RucioStorageElement

class Rucio:
    def __init__(self):
        self.rse_list = []
        self.rse_by_name = {}

        self.file_list = []
        self.file_by_name = {}

    def get_rse_obj(self, rse):
        rse_obj = None
        if isinstance(rse, str):
            rse_obj = self.rse_by_name.get(rse)
            if not rse_obj:
                raise LookupError('rse name {} is not registered'.format(rse))
        elif isinstance(rse, RucioStorageElement):
            rse_obj = rse
        else:
            raise TypeError('rse must be either rse name or rse object')
        return rse_obj

    def get_file_obj(self, file):
        file_obj = None
        if isinstance(file, str):
            file_obj = self.file_by_name.get(file)
            if not file_obj:
                raise LookupError('file name {} is not registered'.format(file))
        elif isinstance(file, File):
            file_obj = file
        else:
            raise TypeError('file must be either file name or file object')
        return file_obj

    def add_rse(self, rse_obj):
        self.rse_list.append(rse_obj)
        self.rse_by_name[rse_obj.name] = rse_obj

    def create_replica(self, file, rse):
        rse_obj = self.get_rse_obj(rse)
        file_obj = self.get_file_obj(file)
        
        if rse_obj.name in file_obj.rse_by_name:
            raise RuntimeError('Rucio.create_replica: rse {} has already a replica of {}'.format(rse_obj.name, file_obj.name))

        new_replica = Replica(rse_obj, file_obj)
        rse_obj.add_replica(new_replica)
        file_obj.add_replica(new_replica)
        return new_replica

    def create_file(self, file_name, file_size, die_time):
        if file_name in self.file_by_name:
            raise RuntimeError('Rucio.create_file: file name {} is already registerd'.format(file_name))

        new_file = File(file_name, file_size, die_time)
        self.file_list.append(new_file)
        self.file_by_name[file_name] = new_file
        return new_file

    def delete_file(self, file):
        file = self.get_file_obj(file)
        self.file_list.remove(file)
        del self.file_by_name[file.name]
        file.delete()

    def run_reaper(self, cur_time):
        num_files = len(self.file_list)

        i = 0
        k = num_files - 1
        while i <= k:
            item_k = self.file_list[k]
            item_i = self.file_list[i]
            while k > 0 and item_k.die_time <= cur_time:
                k -= 1
                item_k = self.file_list[k]

            if item_i.die_time <= cur_time:
                item_i.delete()
                del self.file_by_name[item_i.name]
                self.file_list[k] = item_i
                self.file_list[i] = item_k
                k -= 1
            i += 1

        if k < num_files - 1:
            del self.file_list[k+1:]

        return num_files - len(self.file_list)

    def create_transfer(self, file, linkselector, src_replica, dst_bucket):
        dst_replica = self.create_replica(file, dst_bucket)
        transfer = Transfer(file, linkselector, src_replica, dst_replica)
        return transfer
