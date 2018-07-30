#!/usr/bin/env python
import logging
import random
import uuid
import time

import simpy

from gacs import abstractions, grid, sim
from gacs.clouds import gcp
from gacs.common import monitoring, utils

import numpy as np
import numpy.random as npr

class TransferDurationGeneratorJJ:
    def __init__(self):
        self.rate = 27 * (2**20)
        self.overhead = 18

    def get_duration(self, transfer):
        size = transfer.file.size
        trf_rate = size / ((size/self.rate)+self.overhead)
        return (size / trf_rate)

    def get_finish_times(now, sizes):
         mb_scaler = (2**20)
         overhead = 18.7
         rate = 27.38 * mb_scaler
         max_rw = 26.3 * mb_scaler
         return [max(60, npr.normal(now + (size/min(max_rw, size/((size/rate)+overhead))), 2)) for size in sizes]

class TransferNumGenerator:
    def __init__(self):
        self.DELAY_BASE = 30
        self.ALPHA = 1/self.DELAY_BASE * np.pi/180 * 0.075
        self.SCALE_OF_SOFTMAX = 15
        self.OFFSET_OF_SOFTMAX = 150
        self.GEN_BUNCH_SIZE = 10000
        self.idx_offset = 0
        self.softmax_values = np.empty(self.GEN_BUNCH_SIZE)
        self.make_values(0)

    def make_values(self, start_val):
        step_size = float(self.DELAY_BASE)
        end_val = start_val + self.GEN_BUNCH_SIZE * step_size
        vals = np.arange(start_val, end_val, step_size)
        vals *= self.ALPHA
        np.cos(vals, out=self.softmax_values)
        self.softmax_values *= self.SCALE_OF_SOFTMAX
        self.softmax_values += self.OFFSET_OF_SOFTMAX
        self.softmax_values += npr.normal(0, 1, len(vals)) * self.softmax_values * 0.02

    def get_num_to_create(self, cur_time, num_active):
        idx = int(cur_time / self.DELAY_BASE)
        idx -= self.idx_offset
        if idx >= len(self.softmax_values):
            print('Gen...')
            self.make_values(cur_time)
            self.idx_offset += self.GEN_BUNCH_SIZE
            idx -= self.GEN_BUNCH_SIZE
        diff_softmax_active = self.softmax_values[idx] - num_active
        if diff_softmax_active <= 0:
            return 0
        return int(diff_softmax_active ** abs(npr.normal(1.05, 0.04)))

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

        self.num_active_transfers = 0
        self.g2c_num_transfers_active = 0

        self.INIT_GRIDLINKS_NUM_MIN = 1
        self.INIT_GRIDLINKS_NUM_MAX = 1
        self.INIT_GRIDLINKS_BW_EXPO_MIN = 27
        self.INIT_GRIDLINKS_BW_EXPO_MAX = 29
        self.INIT_CLOUDLINKS_NUM_MIN = 1
        self.INIT_CLOUDLINKS_NUM_MAX = 1
        self.INIT_CLOUDLINKS_BW_EXPO_MIN = 27
        self.INIT_CLOUDLINKS_BW_EXPO_MAX = 29

        self.TRANSFER_UPDATE_DELAY = 20
        self.DOWNLOAD_UPDATE_DELAY = 10

        self.DATAGEN_WAIT = 24 * 3600
        self.DATAGEN_WAIT_MIN = 5 * 24 * 3600
        self.DATAGEN_WAIT_MAX = 8 * 24 * 3600
        self.DATAGEN_FILES_NUM_MIN = 15000
        self.DATAGEN_FILES_NUM_MAX = 15000
        self.DATAGEN_FILES_SIZE_MIN = 2**28
        self.DATAGEN_FILES_SIZE_MAX = 2**31
        self.DATAGEN_LIFETIME_MIN = 5 * 24 * 3600
        self.DATAGEN_LIFETIME_MAX = 14 * 24 * 3600
        self.DATAGEN_REPLICATION_PERCENT = [0.15, 0.80, 0.05]

        self.JOBFAC_WAIT_MIN = 6 * 3600 #5 * 3600
        self.JOBFAC_WAIT_MAX = 18 * 3600 #24 * 3600
        self.JOBFAC_JOB_NUM_MIN = 100
        self.JOBFAC_JOB_NUM_MAX = 150
        self.JOBFAC_INFILES_NUM_MIN = 1
        self.JOBFAC_INFILES_NUM_MAX = 20

        self.REAPER_WAIT = 600

        self.MONITORING_TRANSFER_WAIT = 5

        self.SIM_DURATION = 65*24*3600
        self.new_transfers = []
        self.active_transfers = []

    def billing_process(self):
        log = self.logger.getChild('billing_proc')
        log.info('Started Billing Proc!', self.sim.now)
        billing_month = 1
        while True:
            yield self.sim.timeout(30*24*3600) # calc bill every month
            log.info('BILLING TIME FOR MONTH {}!'.format(billing_month), self.sim.now)

            bill = self.cloud.process_billing(self.sim.now)
            log.info('CHF {:,.2f} of storage costs'.format(bill['storage_total']), self.sim.now)
            log.info('CHF {:,.2f} of network costs'.format(bill['network_total']), self.sim.now)
            monitoring.OnBillingDone(bill, billing_month)

            billing_month = (billing_month % 13) + 1

    def generate_grid_data(self, cur_time):
        log = self.logger.getChild('datagen')
        total_files_gen = random.randint(self.DATAGEN_FILES_NUM_MIN, self.DATAGEN_FILES_NUM_MAX)
        total_replicas_gen = 0
        total_bytes_gen = 0
        max_num_replicas = len(self.DATAGEN_REPLICATION_PERCENT)
        assert max_num_replicas <= len(self.grid_rses), (max_num_replicas, len(self.grid_rses))
        for num_replicas_idx in range(max_num_replicas):
            num_replicas = num_replicas_idx + 1
            num_files_gen = int(total_files_gen * self.DATAGEN_REPLICATION_PERCENT[num_replicas_idx])
            bytes_without_num_replicas = 0
            for i in range(num_files_gen):
                size = random.randint(self.DATAGEN_FILES_SIZE_MIN, self.DATAGEN_FILES_SIZE_MAX)
                dietime = cur_time + random.randint(self.DATAGEN_LIFETIME_MIN, self.DATAGEN_LIFETIME_MAX)
                f = self.rucio.create_file(str(uuid.uuid4()), size, dietime)

                bytes_without_num_replicas += size
                for rse_obj in random.sample(self.grid_rses, num_replicas):
                    self.rucio.create_replica(f, rse_obj)
                    rse_obj.increase_replica(f, 0, size)
            total_bytes_gen += bytes_without_num_replicas * num_replicas
            total_replicas_gen += num_files_gen * num_replicas

        log.info('Created {} files with {} replicas using {} of space'.format(total_files_gen,
                                                                              total_replicas_gen,
                                                                              utils.sizefmt(total_bytes_gen)), cur_time)

    def transfer_process(self, transfer):
        log = self.logger.getChild('transfer_proc')
        #log.debug('Transfering {} from {} to {}'.format(transfer.file.name, transfer.linkselector.src_site.name, transfer.linkselector.dst_site.name), self.sim.now)

        yield self.sim.timeout(1)

        transfer.begin(self.sim.now)
        self.g2c_num_transfers_active += 1
        yield self.sim.timeout(self.TRANSFER_UPDATE_DELAY)

        while transfer.state == abstractions.Transfer.TRANSFER:
            transfer.update(self.sim.now)
            yield self.sim.timeout(self.TRANSFER_UPDATE_DELAY)
        self.g2c_num_transfers_active -= 1
        transfer.end(self.sim.now)

    def transfer_process2(self):
        log = self.logger.getChild('transfer_proc2')
        #log.debug('Transfering {} from {} to {}'.format(transfer.file.name, transfer.linkselector.src_site.name, transfer.linkselector.dst_site.name), self.sim.now)

        while True:
            for transfer in self.new_transfers:
                transfer.begin(self.sim.now)
                self.active_transfers.append(transfer)
            self.new_transfers.clear()
            yield self.sim.timeout(self.TRANSFER_UPDATE_DELAY)
            complete = []
            for transfer in self.active_transfers:
                if transfer.state != abstractions.Transfer.TRANSFER:
                    complete.append(transfer)
                else:
                    transfer.update(self.sim.now)
            for transfer in complete:
                transfer.end(self.sim.now)
                self.active_transfers.remove(transfer)

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

    def job_process2(self, job):
        log = self.logger.getChild('job_proc')
        log.debug('Started job {}'.format(job.id), self.sim.now)

        # STAGE-IN
        stagein_duration = self.sim.now
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
        stagein_duration = self.sim.now - stagein_duration

        # PAYLOAD
        payload_duration = self.sim.now
        job_runtime = random.randint(600, 2*3600)
        yield self.sim.timeout(job_runtime)
        payload_duration = self.sim.now - payload_duration

        # STAGE-OUT
        stageout_duration = self.sim.now
        dst_rse_obj = job.compute_instance.bucket_obj
        dietime = self.sim.now + 3600*24*7

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
        stageout_duration = self.sim.now - stageout_duration

        return True

    def job_gen_process(self):
        log = self.logger.getChild('job_gen_process')
        log.info('Started job generation process!', self.sim.now)

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
                    self.sim.process(self.job_process2(Job(compute_instance, input_files)))

    def transfer_gen_process(self):
        log = self.logger.getChild('transfer_gen_process')
        log.info('Started transfer generation process!', self.sim.now)

        while True:
            yield self.sim.timeout(30)
            # generate grid -> cloud
            num_active = len(self.active_transfers)
            num_to_create = self.g2c_num_generator.get_num_to_create(self.sim.now, num_active)
            num_to_create_per_rse = max(1, int(num_to_create / len(self.grid_rses)))  # assuming uniform distribution
            total_transfers_created = 0
            for grid_rse_obj in self.grid_rses:
                num_files = min(len(grid_rse_obj.replica_list), num_to_create_per_rse)
                if (num_files + total_transfers_created) > num_to_create:
                    num_files = num_to_create - total_transfers_created
                if num_files <= 0:
                    continue
                total_transfers_created += num_files
                replicas = random.sample(grid_rse_obj.replica_list, num_files)
                cloud_rse_obj = npr.choice(self.cloud.bucket_list)
                for replica in replicas:
                    if cloud_rse_obj.name in replica.file.rse_by_name:
                        #log.warning('{} to {}'.format(grid_rse_obj.name, cloud_rse_obj.name))
                        continue
                    self.new_transfers.append(self.rucio.create_transfer(replica.file, grid_rse_obj, cloud_rse_obj))
            #log.debug('active: {}, to_create: {}, created: 0'.format(num_active, num_to_create), self.sim.now)

            # generate cloud -> cloud
                # 1. same multi regional location
                # 2. between multi regional locations
            # generate cloud -> else

    def grid_data_gen_process(self):
        log = self.logger.getChild('grid_data_gen_process')
        log.info('Started grid data generation process!', self.sim.now)

        while True:
            self.generate_grid_data(self.sim.now)
            #wait = random.randint(self.DATAGEN_WAIT_MIN, self.DATAGEN_WAIT_MAX)
            yield self.sim.timeout(self.DATAGEN_WAIT)

    def reaper_process(self):
        log = self.logger.getChild('reaper_process')
        log.info('Started Reaper process!', self.sim.now)
        while True:
            #monitoring.OnPreReaper(self.sim.now)
            t1 = time.time()
            num_deleted = self.rucio.run_reaper_random2(self.sim.now)
            print(time.time() - t1)
            #monitoring.OnPostReaper(self.sim.now, num_deleted)
            if num_deleted:
                log.info('Reapered {}'.format(num_deleted), self.sim.now)
            yield self.sim.timeout(self.REAPER_WAIT)

    def monitoring_transfer_process(self):
        log = self.logger.getChild('monitoring_transfer_process')
        log.info('Started Monitoring-Transfer process!', self.sim.now)
        while True:
            monitoring.OnMonitorTransfer(self.sim.now, self.g2c_num_transfers_active)
            yield self.sim.timeout(self.MONITORING_TRANSFER_WAIT)

    def init_simulation(self):
        log = self.logger.getChild('sim_init')
        random.seed(42)

        log.info('Initialising transfer generators')
        self.g2c_num_generator = TransferNumGenerator()
        #self.g2c_num_generator.duration_generator = TransferDurationGeneratorJJ()
        #self.c2g_num_generator = TransferNumGenerator()
        #self.c2g_num_generator.duration_generator = TransferDurationGeneratorJJ()
        #self.c2c_num_generator = TransferNumGenerator()
        #self.c2c_num_generator.duration_generator = TransferDurationGeneratorJJ()

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
            num_links = random.randint(self.INIT_GRIDLINKS_NUM_MIN, self.INIT_GRIDLINKS_NUM_MAX)
            for i in range(num_links):
                min_bw = 2**self.INIT_GRIDLINKS_BW_EXPO_MIN
                max_bw = 2**(self.INIT_GRIDLINKS_BW_EXPO_MAX - i)
                ls.create_link(random.randint(min_bw, max(min_bw, max_bw)))

            ls = cern_site.create_linkselector(region)
            num_links = random.randint(self.INIT_GRIDLINKS_NUM_MIN, self.INIT_GRIDLINKS_NUM_MAX)
            for i in range(num_links):
                min_bw = 2**self.INIT_GRIDLINKS_BW_EXPO_MIN
                max_bw = 2**(self.INIT_GRIDLINKS_BW_EXPO_MAX - i)
                ls.create_link(random.randint(min_bw, max(min_bw, max_bw)))

            ls = us_site.create_linkselector(region)
            num_links = random.randint(self.INIT_GRIDLINKS_NUM_MIN, self.INIT_GRIDLINKS_NUM_MAX)
            for i in range(num_links):
                min_bw = 2**self.INIT_GRIDLINKS_BW_EXPO_MIN
                max_bw = 2**(self.INIT_GRIDLINKS_BW_EXPO_MAX - i)
                ls.create_link(random.randint(min_bw, max(min_bw, max_bw)))
            self.cloud.create_bucket(region, 'bucket01_{}'.format(region.name), gcp.Bucket.TYPE_REGIONAL)
            #self.cloud.create_bucket(region, 'bucket02_{}'.format(region.name), gcp.Bucket.TYPE_REGIONAL)

        for ls in self.cloud.linkselector_list:
            num_links = random.randint(self.INIT_CLOUDLINKS_NUM_MIN, self.INIT_CLOUDLINKS_NUM_MAX)
            for i in range(num_links):
                min_bw = 2**self.INIT_CLOUDLINKS_BW_EXPO_MIN
                max_bw = 2**(self.INIT_CLOUDLINKS_BW_EXPO_MAX - i)
                ls.create_link(random.randint(min_bw, max(min_bw, max_bw)))

    def simulate(self):
        self.sim.process(self.billing_process())
        self.sim.process(self.grid_data_gen_process())
        #self.sim.process(self.job_gen_process())
        self.sim.process(self.transfer_gen_process())
        self.sim.process(self.transfer_process2())
        self.sim.process(self.reaper_process())
        #self.sim.process(self.monitoring_transfer_process())
        self.sim.run(until=self.SIM_DURATION)

sim = simpy.Environment()
cloud = gcp.Cloud()
rucio = grid.Rucio()
cloud_sim = CloudSimulator(sim, cloud, rucio)
cloud_sim.init_simulation()
cloud_sim.simulate()
monitoring.plotIt()
