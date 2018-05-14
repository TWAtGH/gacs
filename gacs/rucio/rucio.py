
import heapq
import bisect

from gacs.rucio.file import File
from gacs.rucio.rse import RucioStorageElement

import itertools

class Rucio:
    def __init__(self):
        self.rse_list = []
        self.rse_by_name = {}

        self.file_list = []
        self.file_by_name = {}

        self.die_time_prio_counter = itertools.count(1)
        self.die_times = []


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
        return rse_obj.create_replica(file_obj)

    def create_file(self, file_name, file_size, die_time):
        if file_name in self.file_by_name:
            raise RuntimeError('Rucio.create_file: file name {} is already registerd'.format(file_name))

        new_file = File(file_name, file_size, die_time)
        self.file_list.append(new_file)
        self.file_by_name[file_name] = new_file
        # O(log n) search + rebalancing
        # heapq.heappush(self.die_times, (die_time, next(self.die_time_prio_counter), new_file))

        # O(log n) search + insertion
        bisect.insort(self.die_times, (die_time, next(self.die_time_prio_counter), new_file))
        return new_file

    def create_transfer(self, file, dst_rse, linkselector):
        dst_rse = self.get_rse_obj(dst_rse)
        dst_replica = self.create_replica(file, dst_rse)
        transfer = Transfer(file, dst_replica, linkselector)
        return transfer

    def create_download(self, file):
        return Download(file)

    def run_reaper_heap(self, current_time):
        num_files = len(self.file_list)
        while len(self.die_times) and self.die_times[0][0] <= current_time:
            file = self.die_times[0][2]
            file.delete()
            self.file_list.remove(file)
            del self.file_by_name[file.name]
            heapq.heappop(self.die_times)
        return num_files - len(self.file_list)

    def run_reaper_bisect(self, current_time):
        num_files = len(self.file_list)
        if not num_files:
            return 0

        p = bisect.bisect_right(self.die_times, (current_time, 0, None))
        for i in range(p):
            f = self.die_times[i][2]
            assert f.die_time <= current_time
            f.delete(current_time)
            self.file_list.remove(f)
            del self.file_by_name[f.name]
        del self.die_times[0:p]
        return num_files - len(self.file_list)

    def run_reaper_linear(self, current_time):
        num_files = len(self.file_list)

        i = 0
        k = num_files - 1
        while i <= k:
            item_k = self.file_list[k]
            item_i = self.file_list[i]
            while k > 0 and item_k.die_time <= current_time:
                k -= 1
                item_k = self.file_list[k]

            if item_i.die_time <= current_time:
                item_i.delete()
                del self.file_by_name[item_i.name]
                self.file_list[k] = item_i
                self.file_list[i] = item_k
                k -= 1
            i += 1

        if k < num_files - 1:
            del self.file_list[k+1:]
        return num_files - len(self.file_list)
