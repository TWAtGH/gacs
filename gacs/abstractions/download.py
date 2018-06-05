
from gacs.common import utils
from gacs import grid


class Download:
    INIT = 1
    RUNNING = 2
    SUCCESS = 3
    FAILURE = 4

    def __init__(self, src_replica, linkselector):
        self.id = utils.next_id()

        self.start_time = None
        self.end_time = None
        self.last_update_time = None

        self.linkselector = linkselector
        self.link = None
        self.state = self.INIT

    def abort(self):
        assert self.state != self.SUCCESS
        self.state = self.FAILURE

    def begin(self, current_time):
        assert self.state == self.INIT
        assert src_replica.state != grid.Replica.AVAILABLE
        self.start_time = current_time
        self.last_update_time = current_time
        self.link = self.linkselector.alloc_link()
        self.state = self.TRANSFER
        self.file.add_transfer(self)

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
            self.dst_replica.state = grid.Replica.AVAILABLE
            self.file.remove_transfer(self)
