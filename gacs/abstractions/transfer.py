
from gacs.common import monitoring, utils
from gacs import grid


class Transfer:
    INIT = 1
    TRANSFER = 2
    COMPLETE = 3
    DELETED = 4

    def __init__(self, file, linkselector, dst_replica):
        self.id = utils.next_id()
        self.file = file
        self.linkselector = linkselector
        self.dst_replica = dst_replica
        self.dst_rse = dst_replica.rse_obj

        self.start_time = 0
        self.end_time = 0

        self.last_update_time = None
        self.link = None
        self.state = self.INIT

    def delete(self):
        self.state = self.DELETED

    def begin(self, current_time):
        assert self.state == self.INIT
        self.start_time = current_time
        self.last_update_time = current_time
        self.link = self.linkselector.alloc_link()
        self.state = self.TRANSFER
        self.file.add_transfer(self)
        monitoring.OnTransferBegin(self)

    def update(self, current_time):
        assert self.state == self.TRANSFER, self.state
        assert self.last_update_time > 0, self.last_update_time

        time_passed = current_time - self.last_update_time
        assert time_passed > 0, (current_time, self.last_update_time, time_passed)

        self.linkselector.free_link(self.link)
        self.link = self.linkselector.alloc_link()

        src_size = self.file.size
        dst_size = self.dst_replica.size

        bandwidth = int(self.link.bandwidth / self.link.active_transfers)
        transferred = min(bandwidth * time_passed, src_size - dst_size)
        assert transferred > 0, (self.state, bandwidth * time_passed, src_size - dst_size)

        self.dst_rse.increase_replica(self.file, current_time, transferred)
        self.link.used_traffic += transferred
        if src_size == self.dst_replica.size:
            self.state = self.COMPLETE

    def end(self, current_time):
        self.end_time = current_time
        self.linkselector.free_link(self.link)
        if self.state == self.COMPLETE:
            self.file.remove_transfer(self)
        monitoring.OnTransferEnd(self)

    def start_transafer(self):
        pass
    def end_transfer(self):
        pass
