from gacs.rucio.replica import Replica

class Transfer:
    INIT = 1
    TRANSFER = 2
    SRC_LOST = 3
    COMPLETE = 4
    FAILED = 5

    def __init__(self, file, linkselector, src_replica, dst_replica):
        self.id = next(idgen)
        self.file = file
        self.linkselector = linkselector
        self.src_replica = src_replica
        self.dst_replica = dst_replica

        self.last_update_time = None
        self.link = None
        self.state = self.INIT

    def begin(self, current_time):
        if self.state != self.INIT:
            raise RuntimeError('Transfer.begin() must not be called multiple times')
        self.last_update_time = current_time
        self.link = self.linkselector.select_link()
        self.link.active_transfers += 1
        self.dst_replica.state = Replica.TRANSFER
        self.state = self.TRANSFER

    def replace_source(self, current_time, linkselector, src_replica):
        self.last_update_time = current_time
        self.linkselector = linkselector
        self.src_replica = src_replica
        self.state = self.TRANSFER

    def update(self, current_time):
        log = logging.getLogger('transfer_proc')
        if self.state != self.TRANSFER:
            raise RuntimeError('Update was called with state {}'.format(self.state))



        if self.src_replica.state != Replica.COMPLETE:
            log_warning(log, current_time, 'src replica state is {}. Switching transfer to SRC_LOST'.format(self.src_replica.state))
            self.state = self.SRC_LOST
            return None
        if self.dst_replica.state == Replica.DELETED:
            self.state = self.FAILED
            return None

        if self.last_update_time is None:
            log_warning(log, current_time, 'last_update')
            return None

        time_passed = current_time - self.last_update_time
        if time_passed == 0:
            log_warning(log, current_time, 'time_passed is 0 in transfer update')
            return None

        self.link.active_transfers -= 1
        self.link = self.linkselector.select_link()
        self.link.active_transfers += 1
        
        src_size = self.file.size
        dst_size = self.dst_replica.size

        bandwidth = self.link.bandwidth / self.link.active_transfers
        transferred = min(bandwidth * time_passed, src_size - dst_size)

        self.dst_replica.increase(transferred)
        self.link.used_traffic += transferred
        if src_size == dst_size:
            self.state = self.COMPLETE

    def end(self):
        self.link.active_transfers -= 1
        if self.state == self.COMPLETE:
            self.dst_replica.state = Replica.COMPLETE
        elif self.state == self.FAILED:
            self.dst_replica.state = Replica.DELETED
            self.dst_replica.size = 0
        else:
            raise RuntimeError('Transfer.end() called with state {}'.format(self.state))
