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

        self.TRANSFER_UPDATE_DELAY = 10
        self.DOWNLOAD_UPDATE_DELAY = 10

    def billing_process(self):
        log = self.logger.getChild('billing_proc')
        log.info('Started Billing Proc!', self.sim.now)
        billing_month = 1
        while True:
            yield self.sim.timeout(30*24*3600) # calc bill every month
            log.info('BILLING TIME FOR MONTH {}!'.format(billing_month), self.sim.now)

            bill = self.cloud.process_billing(self.sim.now)
            log.info('CHF {} of storage costs'.format(bill['storage_total']), self.sim.now)
            log.info('CHF {} of network costs'.format(bill['network_total']), self.sim.now)

            billing_month = (billing_month % 13) + 1

    def transfer_process(self, transfer):
        log = self.logger.getChild('transfer_proc')
        log.debug('Starting transfer: File {} from {} to {}'.format(transfer.file.name, transfer.src_replica.rse_obj.name, transfer.dst_replica.rse_obj.name), self.sim.now)
        transfer.begin(self.sim.now)
        while transfer.state == Transfer.TRANSFER:
            yield self.sim.timeout(self.TRANSFER_UPDATE_DELAY)
            transfer.update(self.sim.now)
        transfer.end()

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

    def direct_copy_stagein_serial(self, job):  # copy input files directly to compute instance
        # + compute instance costs
        for file in job.input_files:
            download = self.rucio.create_download(file)
            while download.is_running():
                download.update(self.sim.now)
                yield self.sim.timeout(self.DOWNLOAD_UPDATE_DELAY)
            if not download.is_successful():
                return False

    def replica_stagein(self, job):  # create replica in cloud storage
        transfer_procs = []
        for f in job.input_files:
            src_rse = random.choice(f.rse_list)
            dst_rse =  job.compute_instance.bucket_obj
            t = self.rucio.create_transfer(f, src_rse, dst_rse)
            transfer_procs.append(self.sim.process(self.transfer_process(t)))

        yield self.sim.all_of(transfer_procs)

    def stagein_process(self, job):
        log = self.logger.getChild('job_proc')
        #log.debug('Staging-IN job {}: {} files'.format(job.id, len(job.input_files)), self.sim.now)
        for f in job.input_files:
            self.rucio.download(job.compute_instance, input_files)

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
        # log.debug('Staging-OUT job {}: {} files'.format(job.id, len(job.output_files)), self.sim.now)
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
        # log.debug('Started job {}'.format(job.id), self.sim.now)
        
        state = False
        if True:
            stagein_duration = self.sim.now
            #state = yield self.sim.process(self.direct_copy_stagein_serial(job))
            state = yield self.sim.process(self.replica_stagein(job))
            stagein_duration = self.sim.now - stagein_duration
            #self.add_approx_compute_costs(stagein_duration)
        else:
            state = yield self.sim.process(self.replica_stagein(job))

        if state == False:
            log.error('Stage-IN failed. Cannot run job {}'.format(job.id), self.sim.now)
            return False

        job_runtime = random.randint(1800, 36000)
        yield self.sim.timeout(job_runtime)
        for f in job.input_files: 
            output_name = 'out_j{}_i{}'.format(job.id, f.name)
            size = random.randint(2**29, 2**32)
            out_file = self.rucio.create_file(output_name, size, self.sim.now + 3600*24*14)
            replica = self.rucio.create_replica(out_file, random.choice(self.cloud.bucket_list))
            replica.rse_obj.increase_replica(out_file, self.sim.now, size)
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
            total_file_count = len(self.rucio.file_list)
            total_region_count = len(self.cloud.region_list)
            log.info('{} files left'.format(total_file_count))
            assert total_file_count, 'Cannot generate jobs. No files registered.'
            assert total_region_count, 'Cannot generate jobs. No regions registered.'

            num_files = min(random.randint(100, 200), total_file_count)
            input_files = random.sample(rucio.file_list, num_files)
            bucket = random.choice(self.cloud.bucket_list)
            compute_instance = ComputeInstance(bucket)
            for file in input_files:
                self.sim.process(self.job_process(Job(compute_instance, [file])))

    def reaper_process(self):
        log = self.logger.getChild('reaper_process')
        log.info('Started Reaper Process!', self.sim.now)
        while True:
            num_deleted = self.rucio.run_reaper_bisect(self.sim.now)
            yield self.sim.timeout(300)

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
            f = self.rucio.create_file(str(uuid.uuid4()), size, random.randint(3600*24*7, 3600*24*14))
            replica = self.rucio.create_replica(f, random.choice(self.cloud.bucket_list))
            replica.rse_obj.increase_replica(f, 0, size)

    def simulate(self):
        self.sim.process(self.billing_process())
        self.sim.process(self.job_factory())
        self.sim.process(self.reaper_process())
        self.sim.run(until=95*24*3600)

from gacs.clouds.gcp import GoogleCloud
from gacs.rucio.rucio import Rucio
sim = simpy.Environment()
cloud = GoogleCloud()
rucio = Rucio()
cloud_sim = CloudSimulator(sim, cloud, rucio)
cloud_sim.init_simulation()
cloud_sim.simulate()
