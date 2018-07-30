from gacs import abstractions
from gacs.common import utils

import time

data = None
class MonitoringData:
    def __init__(self):
        self.costs_storage = []
        self.costs_network = []
        self.transfer_num_completed = 0
        self.transfer_num_deleted = 0
        self.transfer_duration = []
        self.transfer_size = []
        self.tick_times = []
        self.num_active_transfers = []
        self.reaper_duration = []
        self.num_files = []
        self.storage_graph = ([], [])
        self.storage_graph_indices = {}


def init():
    global data
    data = MonitoringData()


def OnTransferBegin(transfer):
    pass


def OnTransferEnd(transfer):
    if transfer.state == abstractions.Transfer.COMPLETE:
        data.transfer_num_completed += 1
    elif transfer.state == abstractions.Transfer.DELETED:
        data.transfer_num_deleted += 1
    data.transfer_duration.append(transfer.end_time - transfer.start_time)
    data.transfer_size.append(transfer.file.size)


def OnFileDeletion(file):
    pass


def OnCreateTransferGridToCloud(transfer):
    pass


def OnCreateTransferCloudToCloud(transfer):
    pass


def OnCloudStorageVolumeChange(bucket, time, volume):
    idx = data.storage_graph_indices.get(bucket.name)
    if not idx:
        idx = len(data.storage_graph_indices)
        data.storage_graph_indices[bucket.name] = idx
        data.storage_graph[0].append([])
        data.storage_graph[1].append([])
    data.storage_graph[0][idx].append(time)
    data.storage_graph[1][idx].append(volume)


def OnBillingDone(bill, month):
    data.costs_storage.append(bill['storage_total'])
    data.costs_network.append(bill['network_total'])


def OnMonitorTick(current_time, num_active_transfers, last_reaper_duration, num_files):
    data.tick_times.append(current_time)
    data.num_active_transfers.append(num_active_transfers)
    data.reaper_duration.append(last_reaper_duration)
    data.num_files.append(num_files)


def plotIt():
    import matplotlib.pyplot as plt
    import statistics
    print('NumComplete:    {:,d}'.format(data.transfer_num_completed))
    print('NumDeleted:     {:,d}'.format(data.transfer_num_deleted))
    min_transfer = utils.sizefmt(min(data.transfer_size))
    max_transfer = utils.sizefmt(max(data.transfer_size))
    avg_transfer = statistics.mean(data.transfer_size)
    print('MinTransferred: {}'.format(min_transfer))
    print('MaxTransferred: {}'.format(max_transfer))
    print('AvgTransferred: {}'.format(utils.sizefmt(avg_transfer)))
    min_duration = min(data.transfer_duration)
    max_duration = max(data.transfer_duration)
    print('MinDuration:    {}'.format(min_duration))
    print('MaxDuration:    {}'.format(max_duration))
    print('AvgDuration:    {:,.2f}'.format(statistics.mean(data.transfer_duration)))

    plt.figure(1)
    plt.plot(data.costs_storage, label='storage costs')
    plt.plot(data.costs_network, label='newtork costs')
    plt.ylabel('costs/CHF')
    plt.xlabel('time/month')

    plt.figure(2)
    plt.plot(data.tick_times, data.num_active_transfers)
    plt.legend(['NumActiveTransfers'])
    plt.xlabel('time')

    plt.figure(3)
    plt.plot(data.tick_times, data.reaper_duration)
    plt.legend(['ReaperDuration'])
    plt.xlabel('time')

    plt.figure(4)
    plt.plot(data.tick_times, data.num_files)
    plt.legend(['NumFiles'])
    plt.xlabel('time')

    plt.figure(5)
    for k in data.storage_graph_indices:
        idx = data.storage_graph_indices[k]
        plt.plot(data.storage_graph[0][idx], data.storage_graph[1][idx], label=k)
    plt.ylabel('volume GiB')
    plt.xlabel('time')
    plt.legend()

    plt.show()
