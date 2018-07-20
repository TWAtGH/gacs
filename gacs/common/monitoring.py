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
        self.transfer_history_count = []
        self.deletion_time_start = 0


def init():
    global data
    data = MonitoringData()


def OnDownloadBegin(download):
    pass


def OnDownloadEnd(download):
    pass


def OnUploadBegin(upload):
    pass


def OnUploadEnd(upload):
    pass


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


def OnBillingDone(bill, month):
    data.costs_storage.append(bill['storage_total'])
    data.costs_network.append(bill['network_total'])


def OnPreReaper(current_time):
    data.deletion_time_start = time.time()


def OnPostReaper(current_time, num_deleted):
    if num_deleted > 0:
        deletion_duration = time.time() - data.deletion_time_start
        #print('Deleted {} files in {:.2f}s'.format(num_deleted, deletion_duration))

def OnMonitorTransfer(current_time, num_active_transfers):
    data.transfer_history_count.append(num_active_transfers)

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
    plt.plot(data.transfer_history_count, label='active transfers')
    plt.ylabel('count')
    plt.xlabel('time')

    plt.show()
