
from copy import deepcopy

from gacs.rucio.rse import RucioStorageElement
from gacs.sal.link_selector import StorageLinkSelector
import gacs.common.utils

class GoogleBucket(RucioStorageElement):
    TYPE_MULTI = 1
    TYPE_REGIONAL = 2
    TYPE_NEARLINE = 3
    TYPE_COLDLINE = 4

    def __init__(self, region_obj, bucket_name, storage_type):
        super().__init__(bucket_name)
        self.region_obj = region_obj
        self.storage_type = storage_type

    def on_replica_increased(self, replica, amount):
        super().on_replica_increased(replica, amount)

class GoogleRegion:
    def __init__(self, region_name, location_desc, multi_locations, storage_price_chf, sku_id):
        self.name = region_name
        self.location_desc = location_desc
        self.multi_locations = multi_locations
        self.storage_price_chf = storage_price_chf
        self.sku_id = sku_id

        self.linkselector_by_name = {}
        self.bucket_by_name = {}

    def create_linkselector(self, dst_region_obj):
        linkselector = StorageLinkSelector(self, dst_region_obj)
        dst_name = dst_region_obj.name
        if dst_name in self.linkselector_by_name:
            raise RuntimeError('Linkselector from {} to {} already registered'.format(self.name, dst_name))
        self.linkselector_by_name[dst_name] = linkselector
        return linkselector

    def create_bucket(self, bucket_name, storage_type):
        new_bucket = GoogleBucket(self, bucket_name, storage_type)
        self.bucket_by_name[bucket_name] = new_bucket
        return new_bucket

class GoogleCloud:
    def __init__(self):
        self.region_list = []
        self.region_by_name = {}

        self.bucket_list = []
        self.bucket_by_name  = {}

        self.linkselector_list = []
        #self.linkselector_by_id = {}

        self.transfer_list = []
        #self.transfer_by_id = {}

        self.multi_locations = {}

    def is_same_location(self, region1, region2):
        return region1.name == region2.name

    def is_same_multi_location(self, region1, region2):
        if region1.name in region2.multi_locations:
            return not self.is_same_location(region1, region2)
        return False

    def setup_default_regions(self):
        if len(self.region_list):
            raise RuntimeError('Default regions for cloud obj {} are already set'.format(self.name))

        self.multi_locations['asia'] = ['asia', 'asia-northeast1', 'asia-south1', 'asia-east1', 'asia-southeast1']
        self.multi_locations['europe'] = ['europe', 'europe-west1', 'europe-west2', 'europe-west3', 'europe-west4']
        self.multi_locations['us'] = ['us', 'us-central1', 'us-west1', 'us-east1', 'us-east4', 'northamerica-northeast1']
        self.multi_locations['southamerica-east1'] = ['southamerica-east1']
        #self.multi_locations['northamerica-northeast1'] = ['northamerica-northeast1']
        self.multi_locations['australia-southeast1'] = ['australia-southeast1']

        self.create_region('asia', 'asia',             'Asia',      0.02571790, 'E653-0A40-3B69')
        self.create_region('asia', 'asia-northeast1',  'Tokyo',     0.02275045, '1845-1496-2891')
        self.create_region('asia', 'asia-east1',       'Taiwan',    0.01978300, 'BAE2-255B-64A7')
        self.create_region('asia', 'asia-southeast1',  'Singapore', 0.01978300, '76BA-5CAD-4338')
        self.create_region('asia', 'asia-south1',      'Mumbai',    0.02275045, '2717-BEFE-3773')

        self.create_region('europe', 'europe',       'Europe',      0.02571790, 'EC40-8747-D6FF')
        self.create_region('europe', 'europe-west1', 'Belgium',     0.01978300, 'A703-5CB6-E0BF')
        self.create_region('europe', 'europe-west2', 'London',      0.02275045, 'BB55-3E5A-405C')
        self.create_region('europe', 'europe-west3', 'Frankfurt',   0.02275045, 'F272-7933-F065')
        self.create_region('europe', 'europe-west4', 'Netherlands', 0.01978300, '89D8-0CF9-9F2E')

        self.create_region('us', 'us',          'US',                0.02571790, '0D5D-6E23-4250')
        self.create_region('us', 'us-central1', 'Iowa',              0.01978300, 'E5F0-6A5D-7BAD')
        self.create_region('us', 'us-west1',    'Oregon',            0.01978300, 'E5F0-6A5D-7BAD')
        self.create_region('us', 'us-east1',    'South Carolina',    0.01978300, 'E5F0-6A5D-7BAD')
        self.create_region('us', 'us-east4',    'Northern Virginia', 0.02275045, '5F7A-5173-CF5B')

        self.create_region('us',   'northamerica-northeast1',  'Montreal',  0.02275045, 'E466-8D73-08F4')
        #self.create_region('northamerica-northeast1',   'northamerica-northeast1',  'Montreal',  0.02275045, 'E466-8D73-08F4')
        self.create_region('southamerica-east1',        'southamerica-east1',       'Sao Paulo', 0.03462025, '6B9B-6AB4-AC59')
        self.create_region('australia-southeast1',      'australia-southeast1',     'Sydney',    0.02275045, 'CF63-3CCD-F6EC')

    def setup_default_linkselectors(self):
        if len(self.region_list) == 0:
            raise RuntimeError('Need regions before creating default linkselectors for cloud obj {}'.format(self.name))
        for src_region in self.region_list:
            for dst_region in self.region_list:
                if src_region == dst_region:
                    continue
                linkselector = src_region.create_linkselector(dst_region)
                self.linkselector_list.append(linkselector)

    def setup_default_networkcosts(self):
        if len(self.region_list) == 0:
            raise RuntimeError('Need regions before setting default network costs for cloud obj {}'.format(self.name))
        if len(self.linkselector_list) == 0:
            raise RuntimeError('Need linkselectors before setting default network costs for cloud obj {}'.format(self.name))

        """
        eu - apac EF0A-B3BA-32CA 0.1121580 0.1121580 0.1028115 0.0747720
        na - apac 6B37-399C-BF69 0.0000000 0.1121580 0.1028115 0.0747720
        na - eu   C7FF-4F9E-C0DB 0.0000000 0.1121580 0.1028115 0.0747720
 
        au - apac CDD1-6B91-FDF8 0.1775835 0.1775835 0.1682370 0.1401975
        au - eu   1E7D-CBB0-AF0C 0.1775835 0.1775835 0.1682370 0.1401975
        au - na   27F0-D54C-619A 0.1775835 0.1775835 0.1682370 0.1401975
        au - sa   7F66-C883-4D7D 0.1121580 0.1121580 0.1028115 0.0747720
        apac - sa 1F9A-A9AC-FFC3 0.1121580 0.1121580 0.1028115 0.0747720
        eu - sa   96EB-C6ED-FBDE 0.1121580 0.1121580 0.1028115 0.0747720
        na - sa   BB86-91E8-5450 0.1121580 0.1121580 0.1028115 0.0747720
        """
        # setup bucket to bucket transfer cost
        cost_same_region    = {0: 0.0000000, 1: 0.0000000, 1024: 0.0000000, 10240: 0.0000000}
        cost_same_multi      = {0: 0.0093465, 1: 0.0093465, 1024: 0.0093465, 10240: 0.0093465}

        cost_ww = {'asia': {}, 'australia-southeast1': {}, 'europe': {}, 'southamerica-east1': {}, 'us': {}}

        cost_ww['asia']['australia-southeast1'] = {0: 0.1775835, 1: 0.1775835, 1024: 0.1682370, 10240: 0.1401975}
        cost_ww['asia']['europe']               = {0: 0.1121580, 1: 0.1121580, 1024: 0.1028115, 10240: 0.0747720}
        cost_ww['asia']['southamerica-east1']   = {0: 0.1121580, 1: 0.1121580, 1024: 0.1028115, 10240: 0.0747720}
        cost_ww['asia']['us']                   = {0: 0.0000000, 1: 0.1121580, 1024: 0.1028115, 10240: 0.0747720}
        
        cost_ww['australia-southeast1']['europe']             = {0: 0.1775835, 1: 0.1775835, 1024: 0.1682370, 10240: 0.1401975}
        cost_ww['australia-southeast1']['southamerica-east1'] = {0: 0.1121580, 1: 0.1121580, 1024: 0.1028115, 10240: 0.0747720}
        cost_ww['australia-southeast1']['us']                 = {0: 0.1775835, 1: 0.1775835, 1024: 0.1682370, 10240: 0.1401975}
        
        cost_ww['europe']['southamerica-east1'] = {0: 0.1121580, 1: 0.1121580, 1024: 0.1028115, 10240: 0.0747720}
        cost_ww['europe']['us']                 = {0: 0.0000000, 1: 0.1121580, 1024: 0.1028115, 10240: 0.0747720}

        cost_ww['southamerica-east1']['us'] = {0: 0.1121580, 1: 0.1121580, 1024: 0.1028115, 10240: 0.0747720}

        cost_tmp = deepcopy(cost_ww)
        for k in cost_tmp:
            for k2 in cost_tmp[k]:
                cost_ww_k2 = cost_ww.setdefault(k2, {})
                cost_ww_k2[k] = cost_tmp[k][k2]

        for linkselector in self.linkselector_list:
            r1 = linkselector.src_region
            r2 = linkselector.dst_region
            same_loc = self.is_same_location(r1, r2)
            same_mloc = self.is_same_multi_location(r1, r2)
            if same_loc == False and same_mloc == False:
                mr1 = mr2 = ''
                for m in self.multi_locations:
                    if r1.name in self.multi_locations[m]:
                        mr1 = m
                    if r2.name in self.multi_locations[m]:
                        mr2 = m
                linkselector.network_price_chf = cost_ww[mr1][mr2]
            elif same_loc:
                linkselector.network_price_chf = cost_same_region
            else:
                linkselector.network_price_chf = cost_same_multi

        #download apac      1F8B-71B0-3D1B 0.0000000 0.1121580 0.1028115 0.0747720
        #download australia 9B2D-2B7D-FA5C 0.1775835 0.1775835 0.1682370 0.1401975
        #download china     4980-950B-BDA6 0.2149695 0.2149695 0.2056230 0.1869300
        #download us emea   22EB-AAE8-FBCD 0.0000000 0.1121580 0.1028115 0.0747720
        """
        for linkselector in grid.rse('asia').linkselector_list:
            linkselector.set_egress_costs(0, 0.112158, 0.1028115, 0.074772)
        for linkselector in grid.rse('europe').linkselector_list:
            linkselector.set_egress_costs(0, 0.112158, 0.1028115, 0.074772)
        for linkselector in grid.rse('us').linkselector_list:
            linkselector.set_egress_costs(0, 0.112158, 0.1028115, 0.074772)
        for linkselector in grid.rse('sydney').linkselector_list:
            linkselector.set_egress_costs(0.1775835, 0.168237, 0.1401975)
        """

    def setup_default_operationcosts(self):
        pass

    def setup_default(self):
        self.setup_default_regions()
        self.setup_default_linkselectors()
        self.setup_default_networkcosts()
        self.setup_default_operationcosts()

    def get_as_graph(self):
        graph = {}
        for src_bucket in self.bucket_list:
            src_name = src_bucket.name
            src_region = src_bucket.region_obj
            graph[src_name] = {}
            for dst_bucket in self.bucket_list:
                dst_name = dst_bucket.name
                dst_region = dst_bucket.region_obj
                w = 0
                ls = src_region.linkselector_by_name.get(dst_region.name)
                if ls != None:
                    w = ls.get_weight()
                graph[src_name][dst_name] = w
        return graph

    def create_region(self, multi_location, region_name, location_desc, storage_price_chf, sku_id):
        # should the multi locs contain location_name????
        mul_locs = self.multi_locations[multi_location]
        new_region = GoogleRegion(region_name, location_desc, mul_locs, storage_price_chf, sku_id)
        self.region_list.append(new_region)
        self.region_by_name[new_region.name] = new_region
        return new_region

    def get_region_obj(self, region):
        region_obj = None
        if isinstance(region, str):
            region_obj = self.region_by_name.get(region)
            if not region_obj:
                raise LookupError('region name {} is not registered'.format(region))
        elif isinstance(region, GoogleRegion):
            region_obj = region
        else:
            raise TypeError('region must be either region name or region object')
        return region_obj

    def get_bucket_obj(self, bucket):
        bucket_obj = None
        if isinstance(bucket, str):
            bucket_obj = self.bucket_by_name.get(bucket)
            if not bucket_obj:
                raise LookupError('bucket name {} is not registered'.format(bucket))
        elif isinstance(bucket, GoogleBucket):
            bucket_obj = bucket
        else:
            raise TypeError('bucket must be either bucket name or bucket object')
        return bucket

    def create_bucket(self, region, bucket_name, storage_type):
        if bucket_name in self.bucket_by_name:
            raise RuntimeError('GoogleCloud.create_bucket: bucket name {} is already registerd'.format(bucket_name))

        region_obj = self.get_region_obj(region)
        if storage_type == GoogleBucket.TYPE_MULTI:
            region_name = region_obj.name
            if region_name not in ['asia', 'europe', 'us']: # TODO needs better solution!
                raise RuntimeError('GoogleCloud.create_bucket: cannot create multi regional bucket in region {}'.format(region_name))

        new_bucket = region_obj.create_bucket(bucket_name, storage_type)

        self.bucket_list.append(new_bucket)
        self.bucket_by_name[bucket_name] = new_bucket
        return new_bucket
