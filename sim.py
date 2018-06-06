#!/usr/bin/env python
import logging
import random
import uuid

import simpy

from gacs import abstractions, grid, sim
from gacs.clouds import gcp
from gacs.common import monitoring, utils


class ComputeInstance:
    def __init__(self, bucket_obj):
        self.bucket_obj = bucket_obj

class Job:
    def __init__(self, compute_instance, input_files):
        self.id = utils.next_id()
        self.compute_instance = compute_instance
        self.input_files = input_files
        self.output_files = []

class CloudSimulator(sim.BaseSim):
    def __init__(self, sim, cloud, rucio):
        super().__init__()
        self.sim = sim
        self.cloud = cloud
        self.rucio = rucio

        self.TRANSFER_UPDATE_DELAY = 10
        self.DOWNLOAD_UPDATE_DELAY = 10

        self.DATAGEN_WAIT_MIN = 7 * 24 * 3600
        self.DATAGEN_WAIT_MAX = 7 * 24 * 3600
        self.DATAGEN_FILES_NUM_MIN = 15000
        self.DATAGEN_FILES_NUM_MAX = 15000
        self.DATAGEN_FILES_SIZE_MIN = 2**27
        self.DATAGEN_FILES_SIZE_MAX = 2**31
        self.DATAGEN_LIFETIME_MIN = 7 * 24 * 3600
        self.DATAGEN_LIFETIME_MAX = 14 * 24 * 3600
        self.DATAGEN_REPLICATION_PERCENT = [0.18, 0.69, 0.13]

        self.JOBFAC_WAIT_MIN = 5 * 3600
        self.JOBFAC_WAIT_MAX = 24 * 3600
        self.JOBFAC_JOB_NUM_MIN = 50
        self.JOBFAC_JOB_NUM_MAX = 150
        self.JOBFAC_INFILES_NUM_MIN = 1
        self.JOBFAC_INFILES_NUM_MAX = 15

        self.REAPER_SLEEP = 300

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
            monitoring.OnBillingDone(bill, billing_month)

            billing_month = (billing_month % 13) + 1

    def generate_grid_data(self, cur_time):
        total_files_stored = random.randint(self.DATAGEN_FILES_NUM_MIN, self.DATAGEN_FILES_NUM_MAX)
        total_replicas_stored = 0
        total_bytes_stored = 0
        max_num_replicas = len(self.DATAGEN_REPLICATION_PERCENT)
        assert num_replicas > len(self.grid_rses), (num_replicas, len(self.grid_rses))
        for num_replicas in range(max_num_replicas):
            file_gen_num = total_file_gen_num * self.DATAGEN_REPLICATION_PERCENT[num_replicas]
            total_replicas_stored += file_gen_num * num_replicas
            for i in range(file_gen_num):
                size = random.randint(self.DATAGEN_FILES_SIZE_MIN, self.DATAGEN_FILES_SIZE_MAX)
                dietime = cur_time + random.randint(self.DATAGEN_LIFETIME_MIN, self.DATAGEN_LIFETIME_MAX)
                f = self.rucio.create_file(str(uuid.uuid4()), size, dietime)

                total_bytes_stored += size * num_replicas
                for rse_obj in random.sample(self.grid_rses, num_replicas):
                    self.rucio.create_replica(f, rse_obj)
                    rse_obj.increase_replica(f, 0, size)

        log.info('Created {} files with {} replicas using {} of space'.format(total_files_stored,
                                                                              total_replicas_stored,
                                                                              utils.sizefmt(total_bytes_stored)))

    def transfer_process(self, transfer):
        log = self.logger.getChild('transfer_proc')
        log.debug('Transfering {} from {} to {}'.format(transfer.file.name, transfer.linkselector.src_site.name, transfer.linkselector.dst_site.name), self.sim.now)

        transfer.begin(self.sim.now)
        yield self.sim.timeout(self.TRANSFER_UPDATE_DELAY)

        while transfer.state == abstractions.Transfer.TRANSFER:
            transfer.update(self.sim.now)
            yield self.sim.timeout(self.TRANSFER_UPDATE_DELAY)

        transfer.end(self.sim.now)

    def download_process(self, download):
        log = self.logger.getChild('download_proc')
        #log.debug('Transfering {} from {} to {}'.format(transfer.file.name, transfer.linkselector.src_site.name, transfer.linkselector.dst_site.name), self.sim.now)

        download.begin(self.sim.now)
        yield self.sim.timeout(self.DOWNLOAD_UPDATE_DELAY)

        while download.state == abstractions.Download.TRANSFER:
            download.update(self.sim.now)
            yield self.sim.timeout(self.DOWNLOAD_UPDATE_DELAY)

        download.end(self.sim.now)

    def stagein_process(self, job):
        log = self.logger
        direct_copy = False

        copy_procs = []
        dst_rse_obj = job.compute_instance.bucket_obj
        for file_obj in job.input_files:
            if not direct_copy and dst_rse_obj.name in file_obj.rse_by_name:
                # file already exists at dst
                continue

            available_src_replicas = []
            for replica_obj in file_obj.replica_list:
                if replica_obj.state == grid.Replica.AVAILABLE:
                    available_src_replicas.append(replica_obj)
            #TODO: replace shuffle with replica sorting
            random.shuffle(available_src_replicas)
            if len(available_src_replicas) == 0:
                log.error('Failed to stagein file: No replicas available', self.sim.now)
                return False

            src_replica_obj = available_src_replicas[0]
            if direct_copy:
                # WIP code; need to add proper linkselectors
                download = self.rucio.create_download(src_replica_obj, dst_rse_obj.site_obj)
                copy_procs.append(self.sim.process(self.download_process(download)))
            else:
                transfer = self.rucio.create_transfer(file_obj, src_replica_obj.rse_obj, dst_rse_obj)
                copy_procs.append(self.sim.process(self.transfer_process(transfer)))

        if len(copy_procs) > 0:
            yield self.sim.all_of(copy_procs)
        return True

    def runpayload_process(self, job):
        job_runtime = random.randint(1800, 36000)
        yield self.sim.timeout(job_runtime)
        return True

    def stageout_process(self, job):
        dst_rse_obj = job.compute_instance.bucket_obj
        dietime = self.sim.now + 3600*24*14

        # create job output file
        size = random.randint(2**26, 2**30)
        file_obj = self.rucio.create_file('out_res_j{}'.format(job.id), size, dietime)
        replica_obj = self.rucio.create_replica(file_obj, dst_rse_obj)
        dst_rse_obj.increase_replica(file_obj, self.sim.now, size)
        job.output_files.append(file_obj)

        # create job log file
        size = random.randint(2**21, 2**24)
        file_obj = self.rucio.create_file('out_log_j{}'.format(job.id), size, dietime)
        replica_obj = self.rucio.create_replica(file_obj, dst_rse_obj)
        dst_rse_obj.increase_replica(file_obj, self.sim.now, size)
        job.output_files.append(file_obj)

        yield self.sim.timeout(300)
        return True

    def job_process(self, job):
        log = self.logger.getChild('job_proc')
        log.debug('Started job {}'.format(job.id), self.sim.now)

        stagein_duration = self.sim.now
        success = yield self.sim.process(self.stagein_process(job))
        if not success:
            log.error('Stagein failed', self.sim.now)
            return False
        stagein_duration = self.sim.now - stagein_duration

        payload_duration = self.sim.now
        success = yield self.sim.process(self.runpayload_process(job))
        if not success:
            log.error('Payload failed', self.sim.now)
            return False
        payload_duration = self.sim.now - payload_duration

        stageout_duration = self.sim.now
        success = yield self.sim.process(self.stageout_process(job))
        if not success:
            log.error('Stageout failed', self.sim.now)
            return False
        stageout_duration = self.sim.now - stageout_duration
        return True

    def job_factory(self):
        log = self.logger.getChild('job_factory')
        log.info('Started Job Factory!', self.sim.now)

        while True:
            wait = random.randint(self.JOBFAC_WAIT_MIN, self.JOBFAC_WAIT_MAX)
            yield self.sim.timeout(wait)
            total_file_count = len(self.rucio.file_list)
            total_region_count = len(self.cloud.region_list)
            assert total_file_count > 0, total_file_count
            assert total_region_count > 0, total_file_count

            num_jobs = min(random.randint(self.JOBFAC_JOB_NUM_MIN, self.JOBFAC_JOB_NUM_MAX), total_file_count)
            log.debug('{} new jobs, {} registered files, waited {}'.format(num_jobs, total_file_count, wait), self.sim.now)
            for job_nr in range(num_jobs):
                num_input_files = random.randint(self.JOBFAC_INFILES_NUM_MIN, self.JOBFAC_INFILES_NUM_MAX)
                input_files = random.sample(rucio.file_list, num_input_files)
                bucket = random.choice(self.cloud.bucket_list)
                compute_instance = ComputeInstance(bucket)
                for file in input_files:
                    self.sim.process(self.job_process(Job(compute_instance, input_files)))

    def reaper_process(self):
        log = self.logger.getChild('reaper_process')
        log.info('Started Reaper Process!', self.sim.now)
        while True:
            num_deleted = self.rucio.run_reaper_bisect(self.sim.now)
            #log.info('Reapered {}'.format(num_deleted), self.sim.now)
            yield self.sim.timeout(self.REAPER_SLEEP)

    def init_simulation(self):
        log = self.logger.getChild('sim_init')
        random.seed(42)

        self.grid_rses = []
        asia_site = grid.Site('ASGC', ['asia'])
        self.grid_rses.append(asia_site.create_rse('TAIWAN_DATADISK'))
        cern_site = grid.Site('CERN', ['europe'])
        self.grid_rses.append(cern_site.create_rse('CERN_DATADISK'))
        us_site = grid.Site('BNL', ['us'])
        self.grid_rses.append(us_site.create_rse('BNL_DATADISK'))


        self.cloud.setup_default()

        for region in self.cloud.region_list:
            ls = asia_site.create_linkselector(region)
            ls.create_link(2**31)
            ls.create_link(2**31)
            ls = cern_site.create_linkselector(region)
            ls.create_link(2**31)
            ls.create_link(2**31)
            ls = us_site.create_linkselector(region)
            ls.create_link(2**31)
            ls.create_link(2**31)
            self.cloud.create_bucket(region, 'bucket01_{}'.format(region.name), gcp.Bucket.TYPE_REGIONAL)
            #self.cloud.create_bucket(region, 'bucket02_{}'.format(region.name), gcp.Bucket.TYPE_REGIONAL)

        for linkselector in self.cloud.linkselector_list:
            num_links = random.randint(1, 3)
            for i in range(num_links):
                linkselector.create_link(random.randint(2**28, 2**(31-num_links)))

        total_files_stored = 10000
        total_replicas_stored = 0
        total_bytes_stored = 0
        for i in range(total_files_stored):
            size = random.randint(2**28, 2**31)
            f = self.rucio.create_file(str(uuid.uuid4()), size, random.randint(3600*24*8, 3600*24*14))

            rses = []
            num_rses = 1
            replication_chance = random.randint(1, 100)
            if replication_chance < 6:
                num_rses = 3
            elif replication_chance < 21:
                num_rses = 2

            total_replicas_stored += num_rses
            total_bytes_stored += size * num_rses
            for rse_obj in random.sample(self.grid_rses, num_rses):
                self.rucio.create_replica(f, rse_obj)
                rse_obj.increase_replica(f, 0, size)

        log.info('Created {} files with {} replicas using {} of space'.format(total_files_stored,
                                                                              total_replicas_stored,
                                                                              utils.sizefmt(total_bytes_stored)))

    def simulate(self):
        self.sim.process(self.billing_process())
        self.sim.process(self.data_generation_process())
        self.sim.process(self.job_factory())
        self.sim.process(self.reaper_process())
        self.sim.run(until=95*24*3600)

sim = simpy.Environment()
cloud = gcp.Cloud()
rucio = grid.Rucio()
cloud_sim = CloudSimulator(sim, cloud, rucio)
cloud_sim.init_simulation()
cloud_sim.simulate()
