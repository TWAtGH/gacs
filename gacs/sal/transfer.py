
from gacs.common.utils import next_id
from gacs.rucio.replica import Replica


class Transfer:
    INIT = 1
    TRANSFER = 2
    COMPLETE = 3
    EXPIRED = 4

    def __init__(self, file, linkselector, src_replica, dst_replica):
        self.id = next_id()
        self.file = file
        self.linkselector = linkselector
        self.src_replica = src_replica
        self.dst_replica = dst_replica

        self.last_update_time = None
        self.link = None
        self.state = self.INIT

    def begin(self, current_time):
        assert self.state == self.INIT
        self.last_update_time = current_time
        self.link = self.linkselector.select_link()
        self.link.active_transfers += 1
        self.state = self.TRANSFER

    def update(self, current_time):
        assert self.state == self.TRANSFER, 'Update called with wrong state {}'.format(self.state)
        assert self.last_update_time > 0, self.last_update_time

        time_passed = current_time - self.last_update_time
        assert time_passed > 0, '{} - {} = {}'.format(current_time, self.last_update_time, time_passed)

        if self.src_replica.state != Replica.AVAILABLE
        or self.dst_replica.state == Replica.DELETED:
            self.state = self.EXPIRED
            return None

        self.link.active_transfers -= 1
        self.link = self.linkselector.select_link()
        self.link.active_transfers += 1
        
        src_size = self.file.size
        dst_size = self.dst_replica.size

        bandwidth = self.link.bandwidth / self.link.active_transfers
        transferred = min(bandwidth * time_passed, src_size - dst_size)

        self.dst_replica.increase(current_time, transferred)
        self.link.used_traffic += transferred
        if src_size == dst_size:
            self.state = self.COMPLETE

    def end(self):
        self.link.active_transfers -= 1
        if self.state == self.COMPLETE:
            self.dst_replica.state = Replica.AVAILABLE
