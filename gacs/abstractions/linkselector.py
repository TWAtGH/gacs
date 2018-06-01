
from gacs.common import utils

class StorageLink:
    def __init__(self, linkselector, bandwidth):
        self.id = utils.next_id()
        self.linkselector = linkselector
        self.bandwidth = bandwidth # 2**30
        self.used_traffic = 0
        self.active_transfers = 0

    def get_available_bandwidth(self):
        return self.bandwidth / (self.active_transfers + 1)

class LinkSelector:
    def __init__(self, src_site, dst_site):
        self.id = utils.next_id()
        self.src_site = src_site
        self.dst_site = dst_site
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
        assert full_bw >= available_bw, (full_bw, available_bw)
        return (full_bw - available_bw)
        
    def select_link(self):
        assert len(self.link_list) > 0
        best_link = self.link_list[0]
        max_available_bw = 0
        for link in self.link_list:
            bw = link.get_available_bandwidth()
            if bw > max_available_bw:
                max_available_bw = bw
                best_link = link
        return best_link
    
    def alloc_link(self):
        link = self.select_link()
        link.active_transfers += 1
        return link

    def free_link(self, link):
        assert link.active_transfers > 0
        link.active_transfers -= 1
