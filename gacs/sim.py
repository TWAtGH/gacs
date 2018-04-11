#!/usr/bin/env python
import logging
import random

import simpy

from itertools import count
from copy import deepcopy

idgen = count(1)

logging.basicConfig(format='[{simtime:>10} - {levelname:8}]: {message}', style='{', level=logging.INFO)

def log_debug(logger, time, msg):
    logger.debug(msg, extra={'simtime':time})
def log_info(logger, time, msg):
    logger.info(msg, extra={'simtime':time})
def log_warning(logger, time, msg):
    logger.warning(msg, extra={'simtime':time})
def log_error(logger, time, msg):
    logger.error(msg, extra={'simtime':time})
def log_critical(logger, time, msg):
    logger.critical(msg, extra={'simtime':time})

def log(time, msg):
    print('[{:5d}] {}'.format(time, msg))

class ComputeInstance:
    def __init__(self, bucket_obj):
        self.bucket_obj = bucket_obj

class Job:
    def __init__(self, compute_instance, input_files):
        self.id = next(idgen)
        self.compute_instance = compute_instance
        self.input_files = input_files
        self.output_files = []

class GridSite(RucioStorageElement):
    def __init__(self, site_name):
        super().__init__(site_name)

    def on_replica_increased(self, replica, amount):
        super().on_replica_increased(replica, amount)

class CloudSimulator:
    def __init__(self, sim, cloud, rucio):
        self.sim = sim
        self.cloud = cloud
        self.rucio = rucio

    def billing_process(self):
        log = logging.getLogger('billing_proc')
        log_info(log, self.sim.now, 'Started Billing Proc!')
        billing_month = 1
        while True:
            yield self.sim.timeout(30*24*3600) # calc bill every month
            log_info(log, self.sim.now, 'BILLING TIME FOR MONTH {}!'.format(billing_month))
            log_info(log, self.sim.now, 'Updating all transfers')
            for transfer in self.cloud.transfer_list:
                transfer.update(sim.now)

            log_info(log, self.sim.now, 'Calculating storage costs')
            storage_costs = {}
            storage_costs_total = 0
            for bucket in self.cloud.bucket_list:
                costs = 0
                #costs = bucket.get_storage_costs()
                #bucket.reset_storage_costs()
                storage_costs[bucket.name] = costs
                storage_costs_total += costs
            log_info(log, self.sim.now, 'CHF {} of storage costs'.format(storage_costs_total))

            log_info(log, self.sim.now, 'Calculating network costs')
            network_costs_total = 0
            for linkselector in self.cloud.linkselector_list:
                costs = 0
                #costs = linkselector.get_traffic_cost()
                #linkselector.reset_traffic_costs()
                network_costs_total += costs
            log_info(log, self.sim.now, 'CHF {} of network costs'.format(network_costs_total))

            billing_month = (billing_month % 13) + 1

    def transfer_process(self, transfer):
        log = logging.getLogger('transfer_proc')
        log_debug(log, self.sim.now, 'Starting transfer: File {} from {} to {}'.format(transfer.file.name, transfer.src_replica.rse_obj.name, transfer.dst_replica.rse_obj.name))
        transfer.begin(self.sim.now)
        while transfer.state == Transfer.TRANSFER:
            yield self.sim.timeout(10)
            transfer.update(self.sim.now)
            if transfer.state == Transfer.SRC_LOST:
                pass
        transfer.end()
        # TODO handle failed transfer

    def find_best_transfers(self, file, dst_bucket):
        graph = self.cloud.get_as_graph()
        sources = []
        for src_replica in file.get_complete_replicas():
            w = graph[src_replica.rse_obj.name][dst_bucket.name]
            if w != 0: # w==0 means no link
                sources.append(src_replica)
                continue 
        sources.sort(key=lambda k: graph[k.rse.name][dst_bucket.name])
        return [[sources]]

    def stagein_process(self, job):
        log = logging.getLogger('job_proc')
        log_debug(log, self.sim.now, 'Staging-IN job {}: {} files'.format(job.id, len(job.input_files)))
        for f in job.input_files:
            if job.compute_instance.bucket_obj.name in f.rse_by_name:
                log_debug(log, self.sim.now, 'Skipping transfer: File already exists')
                continue
            transfer_lists = self.find_best_transfers(f, job.compute_instance.bucket_obj)
            for transfer_list in transfer_lists:
                for src_replica in transfer_list: # TODO

                if len(transfer_list) == 0:
                    log_error(log, self.sim.now, 'Stage-IN job {}: failed to find source for file {}'.format(job.id, f.name))
                    return False
                transfer_procs = []
                for src_replica in transfer_list:
                    dst_bucket = job.compute_instance.bucket_obj
                    linkselector = src_bucket.region_obj.linkselector_by_name[dst_bucket.region_obj.name]
                    transfer = self.rucio.create_transfer(f, linkselector, src_bucket.replica_by_name[f.name], dst_bucket)
                    transfer_procs.append(self.sim.process(self.transfer_process(transfer)))
                yield self.sim.all_of(transfer_procs)

    def stageout_process(self, job):
        log = logging.getLogger('job_proc')
        log_debug(log, self.sim.now, 'Staging-OUT job {}: {} files'.format(job.id, len(job.output_files)))
        transfer_procs = []
        for f in job.output_files:
            # linkselector = ?
            # src_replica = ?
            # dst_bucket = ?
            # transfer = self.rucio.create_transfer(f, linkselector, src_replica, dst_bucket)
            # transfer_procs.append(self.sim.process(self.transfer_process(transfer)))
            pass
        # yield self.sim.all_of(transfer_procs)
        yield self.sim.timeout(120) # pretend stageout

    def job_process(self, job):
        log = logging.getLogger('job_proc')
        log_debug(log, self.sim.now, 'Started job {}'.format(job.id))

        value = yield self.sim.process(self.stagein_process(job))
        if value == False:
            log_error(log, self.sim.now, 'Stage-IN failed. Cannot run job {}'.format(job.id))
            return False
        job_runtime = random.randint(1800, 36000)
        yield self.sim.timeout(job_runtime)
        for f in job.input_files: 
            output_name = 'out_j{}_i{}'.format(job.id, f.name)
            out_file = self.rucio.create_file(output_name, random.randint(2**29, 2**32))
            job.output_files.append(out_file)

        yield self.sim.process(self.stageout_process(job))

    def job_factory(self):
        log = logging.getLogger('job_factory')
        log_info(log, self.sim.now, 'Started Job Factory!')
        min_wait = 5 * 3600
        max_wait = 24 * 3600

        while True:
            wait = random.randint(min_wait, max_wait)
            yield self.sim.timeout(wait)
            log_info(log, self.sim.now, 'Time for new jobs! Waited {}.'.format(wait))

            total_file_count = len(rucio.file_list)
            total_region_count = len(cloud.region_list)
            if total_file_count == 0:
                log_warning(log, self.sim.now, 'Cannot generate jobs. No files registered.')
                continue
            if total_region_count == 0:
                log_warning(log, self.sim.now, 'Cannot generate jobs. No regions registered.')
                continue

            num_files = min(random.randint(1,100), total_file_count)
            input_files = random.sample(rucio.file_list, num_files)
            bucket = random.choice(self.cloud.bucket_list)
            compute_instance = ComputeInstance(bucket)
            for file in input_files:
                self.sim.process(self.job_process(Job(compute_instance, [file])))

    def init_simulation(self):
        random.seed(42)

        self.cloud.setup_default_regions()
        self.cloud.setup_default_linkselectors()
        self.cloud.setup_default_networkcosts()
        self.cloud.setup_default_operationcosts()

        for region in self.cloud.region_list:
            self.cloud.create_bucket(region, 'bucket01_{}'.format(region.name), GoogleBucket.TYPE_REGIONAL)
            self.cloud.create_bucket(region, 'bucket02_{}'.format(region.name), GoogleBucket.TYPE_REGIONAL)

        for linkselector in self.cloud.linkselector_list:
            num_links = random.randint(1,3)
            for i in range(num_links):
                linkselector.create_link(random.randint(2**28,2**(31-num_links))

        total_stored = 0
        import uuid
        for i in range(10000):
            size = random.randint(2**28, 2**32)
            total_stored += size
            f = self.rucio.create_file(str(uuid.uuid4()), size)
            replica = self.rucio.create_replica(f, random.choice(self.cloud.bucket_list))
            replica.size = size
            replica.state = Replica.COMPLETE

    def simulate(self):
        self.sim.process(self.billing_process())
        self.sim.process(self.job_factory())
        self.sim.run(until=65*24*3600)


sim = simpy.Environment()
cloud = GoogleCloud()
rucio = Rucio()
cloud_sim = CloudSimulator(sim, cloud, rucio)
cloud_sim.init_simulation()
cloud_sim.simulate()
