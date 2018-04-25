
from gacs.common.utils import next_id

class StorageLink:
    def __init__(self, linkselector, bandwidth):
        self.id = next_id()
        self.linkselector = linkselector
        self.bandwidth = bandwidth # 2**30
        self.used_traffic = 0
        self.active_transfers = 0

    def get_available_bandwidth(self):
        return self.bandwidth / (self.active_transfers + 1)

class StorageLinkSelector:
    def __init__(self, src_region, dst_region):
        self.id = next_id()
        self.src_region = src_region
        self.dst_region = dst_region
        self.total_transferred = 0
        self.network_price_chf = {0: 0.0000000, 1: 0.0000000, 1024: 0.0000000, 10240: 0.0000000}
        self.link_list = []

    def get_weight(self):
        return 1

    def create_link(self, bandwidth):
        new_link = StorageLink(self, bandwidth)
        self.link_list.append(new_link)
        return new_link

    def calc_full_bandwidth(self):
        bw = 0
        for link in self.link_list:
            bw += link.bandwidth
        return bw

    def calc_available_bandwidth(self):
        bw = 0
        for link in self.link_list:
            bw += link.get_available_bandwidth()
        return bw

    def calc_used_bandwidth(self):
        full_bw = 0
        available_bw = 0
        for link in self.link_list:
            full_bw += link.bandwidth
            available_bw += link.get_available_bandwidth()
        return (full_bw - available_bw)
        
    def select_link(self):
        if len(self.link_list) == 0:
            raise RuntimeError('Need to create link before using select_link')
        best_link = self.link_list[0]
        max_available_bw = 0
        for link in self.link_list:
            bw = link.get_available_bandwidth()
            if bw > max_available_bw:
                max_available_bw = bw
                best_link = link
        return best_link