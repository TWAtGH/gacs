#!/usr/bin/env python
import logging
import random

import simpy

from gacs.clouds.gcp import GoogleBucket

from gacs.sim.basesim import BaseSim
from gacs.rucio.replica import Replica
from gacs.common.logging import SimLogger
from gacs.common.utils import next_id 

class ComputeInstance:
    def __init__(self, bucket_obj):
        self.bucket_obj = bucket_obj

class Job:
    def __init__(self, compute_instance, input_files):
        self.id = next_id()
        self.compute_instance = compute_instance
        self.input_files = input_files
        self.output_files = []
"""
class GridSite(RucioStorageElement):
    def __init__(self, site_name):
        super().__init__(site_name)

    def on_replica_increased(self, replica, amount):
        super().on_replica_increased(replica, amount)
"""
class CloudSimulator(BaseSim):
    def __init__(self, sim, cloud, rucio):
        super().__init__()
        self.sim = sim
        self.cloud = cloud
        self.rucio = rucio

    def billing_process(self):
        log = self.logger.getChild('billing_proc')
        log.info('Started Billing Proc!', self.sim.now)
        billing_month = 1
        while True:
            yield self.sim.timeout(30*24*3600) # calc bill every month
            log.info('BILLING TIME FOR MONTH {}!'.format(billing_month), self.sim.now)
            log.info('Updating all transfers', self.sim.now)
            for transfer in self.cloud.transfer_list:
                transfer.update(sim.now)

            log.info('Calculating storage costs', self.sim.now)
            storage_costs = {}
            storage_costs_total = 0
            for bucket in self.cloud.bucket_list:
                costs = 0
                #costs = bucket.get_storage_costs()
                #bucket.reset_storage_costs()
                storage_costs[bucket.name] = costs
                storage_costs_total += costs
            log.info('CHF {} of storage costs'.format(storage_costs_total), self.sim.now)

            log.info('Calculating network costs', self.sim.now)
            network_costs_total = 0
            for linkselector in self.cloud.linkselector_list:
                costs = 0
                #costs = linkselector.get_traffic_cost()
                #linkselector.reset_traffic_costs()
                network_costs_total += costs
            log.info('CHF {} of network costs'.format(network_costs_total), self.sim.now)

            billing_month = (billing_month % 13) + 1

    def transfer_process(self, transfer):
        log = self.logger.getChild('transfer_proc')
        log.debug('Starting transfer: File {} from {} to {}'.format(transfer.file.name, transfer.src_replica.rse_obj.name, transfer.dst_replica.rse_obj.name), self.sim.now)
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
        log = self.logger.getChild('job_proc')
        log.debug('Staging-IN job {}: {} files'.format(job.id, len(job.input_files)), self.sim.now)
        for f in job.input_files:
            if job.compute_instance.bucket_obj.name in f.rse_by_name:
                log.debug('Skipping transfer: File already exists', self.sim.now)
                continue
            transfer_lists = self.find_best_transfers(f, job.compute_instance.bucket_obj)
            for transfer_list in transfer_lists:
                for src_replica in transfer_list: # TODO
                    pass

                if len(transfer_list) == 0:
                    log.error('Stage-IN job {}: failed to find source for file {}'.format(job.id, f.name), self.sim.now)
                    return False
                transfer_procs = []
                for src_replica in transfer_list:
                    dst_bucket = job.compute_instance.bucket_obj
                    linkselector = src_bucket.region_obj.linkselector_by_name[dst_bucket.region_obj.name]
                    transfer = self.rucio.create_transfer(f, linkselector, src_bucket.replica_by_name[f.name], dst_bucket)
                    transfer_procs.append(self.sim.process(self.transfer_process(transfer)))
                yield self.sim.all_of(transfer_procs)

    def stageout_process(self, job):
        log = self.logger.getChild('job_proc')
        log.debug('Staging-OUT job {}: {} files'.format(job.id, len(job.output_files)), self.sim.now)
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
        log = self.logger.getChild('job_proc')
        log.debug('Started job {}'.format(job.id), self.sim.now)

        value = yield self.sim.process(self.stagein_process(job))
        if value == False:
            log.error('Stage-IN failed. Cannot run job {}'.format(job.id), self.sim.now)
            return False
        job_runtime = random.randint(1800, 36000)
        yield self.sim.timeout(job_runtime)
        for f in job.input_files: 
            output_name = 'out_j{}_i{}'.format(job.id, f.name)
            out_file = self.rucio.create_file(output_name, random.randint(2**29, 2**32))
            job.output_files.append(out_file)

        yield self.sim.process(self.stageout_process(job))

    def job_factory(self):
        log = self.logger.getChild('job_factory')
        log.info('Started Job Factory!', self.sim.now)
        min_wait = 5 * 3600
        max_wait = 24 * 3600

        while True:
            wait = random.randint(min_wait, max_wait)
            yield self.sim.timeout(wait)
            log.info('Time for new jobs! Waited {}.'.format(wait), self.sim.now)

            total_file_count = len(rucio.file_list)
            total_region_count = len(cloud.region_list)
            if total_file_count == 0:
                log.warning('Cannot generate jobs. No files registered.', self.sim.now)
                continue
            if total_region_count == 0:
                log.warning('Cannot generate jobs. No regions registered.', self.sim.now)
                continue

            num_files = min(random.randint(1,100), total_file_count)
            input_files = random.sample(rucio.file_list, num_files)
            bucket = random.choice(self.cloud.bucket_list)
            compute_instance = ComputeInstance(bucket)
            for file in input_files:
                self.sim.process(self.job_process(Job(compute_instance, [file])))

    def init_simulation(self):
        random.seed(42)

        self.cloud.setup_default()

        for region in self.cloud.region_list:
            self.cloud.create_bucket(region, 'bucket01_{}'.format(region.name), GoogleBucket.TYPE_REGIONAL)
            self.cloud.create_bucket(region, 'bucket02_{}'.format(region.name), GoogleBucket.TYPE_REGIONAL)

        for linkselector in self.cloud.linkselector_list:
            num_links = random.randint(1,3)
            for i in range(num_links):
                linkselector.create_link(random.randint(2**28,2**(31-num_links)))

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

from gacs.clouds.gcp import GoogleCloud
from gacs.rucio.rucio import Rucio
sim = simpy.Environment()
cloud = GoogleCloud()
rucio = Rucio()
cloud_sim = CloudSimulator(sim, cloud, rucio)
cloud_sim.init_simulation()
cloud_sim.simulate()