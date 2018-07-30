
from gacs import grid
from gacs.common import monitoring


class Region(grid.Site):
    def __init__(self, name, location_desc, multi_locations, storage_price_chf, sku_id):
        super().__init__(name, location_desc)

        self.multi_locations = multi_locations
        self.storage_price_chf = storage_price_chf
        self.sku_id = sku_id

    def create_rse(self, rse_name, storage_type):
        new_bucket = Bucket(self, rse_name, storage_type)
        self.rse_by_name[rse_name] = new_bucket
        return new_bucket


class Bucket(grid.StorageElement):
    TYPE_MULTI = 1
    TYPE_REGIONAL = 2
    TYPE_NEARLINE = 3
    TYPE_COLDLINE = 4

    def __init__(self, region_obj, name, storage_type):
        assert isinstance(region_obj, Region), type(region_obj)
        super().__init__(region_obj, name)
        self.storage_type = storage_type
        self.time_at_last_reset = 0
        self.storage_at_last_reset = 0
        self.storage_events = []

    def increase_replica(self, file_obj, current_time, amount):
        event = [current_time, amount]
        self.storage_events.append(event)
        super().increase_replica(file_obj, current_time, amount)

    def remove_replica(self, file_obj, current_time):
        replica_obj = self.replica_by_name[file_obj.name]
        event = [current_time, -(replica_obj.size)]
        self.storage_events.append(event)
        super().remove_replica(file_obj, current_time)

    def process_storage_billing(self, current_time):
        price = self.site_obj.storage_price_chf
        time_offset = self.time_at_last_reset
        used_storage_at_time = self.storage_at_last_reset
        costs = 0
        gb_scale = 1024**3
        month_scale = 30*24*3600
        for event in self.storage_events:
            time_diff = event[0] - time_offset
            assert time_diff >= 0
            if time_diff > 0:
                storage_gb = used_storage_at_time/gb_scale
                time_month = time_diff/month_scale
                costs += storage_gb * time_month * price
                time_offset = event[0]
                monitoring.OnCloudStorageVolumeChange(self, event[0], used_storage_at_time)
            used_storage_at_time += event[1]

        assert used_storage_at_time == self.used_storage, (used_storage_at_time, self.used_storage)

        if time_offset < current_time:
            time_diff = current_time - time_offset

            storage_gb = used_storage_at_time/gb_scale
            time_month = time_diff/month_scale
            costs += storage_gb * time_month * price

        self.time_at_last_reset = current_time
        self.storage_at_last_reset = self.used_storage
        self.storage_events.clear()
        return costs


def sum_price_recursive(traffic, price_info, idx):
    assert traffic >= 0, traffic

    threshold = price_info[idx][0]
    price = price_info[idx][1]
    if idx >= 1:
        threshold -= price_info[idx - 1][0]
    if traffic <= threshold or (idx + 1) >= len(price_info):
        return traffic * price
    costs = threshold * price
    return costs + sum_price_recursive(traffic - threshold, price_info, idx + 1)

class Cloud:
    def __init__(self):
        self.region_list = []
        self.region_by_name = {}

        self.bucket_list = []
        self.bucket_by_name  = {}

        self.linkselector_list = []

        self.transfer_list = []

        self.multi_locations = {}

    def is_same_location(self, region1, region2):
        return region1.name == region2.name

    def is_same_multi_location(self, region1, region2):
        if region1.name in region2.multi_locations:
            return not self.is_same_location(region1, region2)
        return False

    def setup_default_regions(self):
        assert len(self.region_list) == 0, self.name

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
        assert len(self.region_list) > 0, self.name

        for src_region in self.region_list:
            for dst_region in self.region_list:
                linkselector = src_region.create_linkselector(dst_region)
                self.linkselector_list.append(linkselector)

    def setup_default_networkcosts(self):
        assert len(self.region_list) > 0, self.name
        assert len(self.linkselector_list) > 0, self.name

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
        cost_same_region    = [(0, 0)]
        cost_same_multi      = [(1, 0.0093465)]

        cost_ww = {'asia': {}, 'australia-southeast1': {}, 'europe': {}, 'southamerica-east1': {}, 'us': {}}

        cost_ww['asia']['australia-southeast1'] = [(1024, 0.1775835), (10240, 0.1682370), (10240, 0.1401975)]
        cost_ww['asia']['europe']               = [(1024, 0.1121580), (10240, 0.1028115), (10240, 0.0747720)]
        cost_ww['asia']['southamerica-east1']   = [(1024, 0.1121580), (10240, 0.1028115), (10240, 0.0747720)]
        cost_ww['asia']['us']                   = [(1, 0.0000000), (1024, 0.1121580), (10240, 0.1028115), (10240, 0.0747720)]

        cost_ww['australia-southeast1']['europe']             = [(1024, 0.1775835), (10240, 0.1682370), (10240, 0.1401975)]
        cost_ww['australia-southeast1']['southamerica-east1'] = [(1024, 0.1121580), (10240, 0.1028115), (10240, 0.0747720)]
        cost_ww['australia-southeast1']['us']                 = [(1024, 0.1775835), (10240, 0.1682370), (10240, 0.1401975)]

        cost_ww['europe']['southamerica-east1'] = [(1024, 0.1121580), (10240, 0.1028115), (10240, 0.0747720)]
        cost_ww['europe']['us']                 = [(1, 0.0000000), (1024, 0.1121580), (10240, 0.1028115), (10240, 0.0747720)]

        cost_ww['southamerica-east1']['us'] = [(1024, 0.1121580), (10240, 0.1028115), (10240, 0.0747720)]
        for linkselector in self.linkselector_list:
            r1 = linkselector.src_site
            r2 = linkselector.dst_site
            same_loc = self.is_same_location(r1, r2)
            same_mloc = self.is_same_multi_location(r1, r2)
            if same_loc == False and same_mloc == False:
                # 1. case: two different multi regions
                # search both multi location names
                mr1_name = mr2_name = ''
                for ml_name in self.multi_locations:
                    if r1.name in self.multi_locations[ml_name]:
                        mr1_name = ml_name
                    if r2.name in self.multi_locations[ml_name]:
                        mr2_name = ml_name
                assert len(mr1_name) and len(mr2_name), (mr1_name, mr2_name)

                # determine order of multi location names for cost_ww dict
                mr = cost_ww.get(mr1_name)
                costs = mr.get(mr2_name)
                if not costs:
                    mr = cost_ww.get(mr2_name)
                    costs = mr.get(mr1_name)
                assert costs, (mr1_name, mr2_name)

                linkselector.network_price_chf = costs
            elif same_loc:
                # 2. case: r1 and r2 are the same region
                linkselector.network_price_chf = cost_same_region
            else:
                # 3. case: region r1 is inside the multi region r2
                linkselector.network_price_chf = cost_same_multi

        #download apac      1F8B-71B0-3D1B 0.0000000 0.1121580 0.1028115 0.0747720
        #download australia 9B2D-2B7D-FA5C 0.1775835 0.1775835 0.1682370 0.1401975
        #download china     4980-950B-BDA6 0.2149695 0.2149695 0.2056230 0.1869300
        #download us emea   22EB-AAE8-FBCD 0.0000000 0.1121580 0.1028115 0.0747720

    def setup_default_operationcosts(self):
        pass

    def setup_default(self):
        self.setup_default_regions()
        self.setup_default_linkselectors()
        self.setup_default_networkcosts()
        self.setup_default_operationcosts()

    def process_billing(self, current_time):
        bill = {}
        for transfer in self.transfer_list:
            transfer.update(current_time)

        storage_costs = {}
        storage_costs_total = 0
        for bucket in self.bucket_list:
            costs = bucket.process_storage_billing(current_time)
            storage_costs[bucket.name] = costs
            storage_costs_total += costs
        bill['storage_per_bucket'] = storage_costs
        bill['storage_total'] = storage_costs_total

        network_costs_total = 0
        for linkselector in self.linkselector_list:
            costs = 0
            traffic = 0
            for link in linkselector.link_list:
                assert link.used_traffic >= 0, link.used_traffic
                traffic += link.used_traffic
                link.used_traffic = 0

            traffic /= 1024**3 # scale from Bytes to GiB
            price_info = linkselector.network_price_chf
            costs = sum_price_recursive(traffic, price_info, 0)
            network_costs_total += costs
        bill['network_total'] = network_costs_total

        return bill

    def get_as_graph(self):
        graph = {}
        for src_bucket in self.bucket_list:
            src_name = src_bucket.name
            src_region = src_bucket.site_obj
            graph[src_name] = {}
            for dst_bucket in self.bucket_list:
                dst_name = dst_bucket.name
                dst_region = dst_bucket.site_obj
                w = 0
                ls = src_region.linkselector_by_name.get(dst_region.name)
                if ls != None:
                    w = ls.get_weight()
                graph[src_name][dst_name] = w
        return graph

    def create_region(self, multi_location, region_name, location_desc, storage_price_chf, sku_id):
        mul_locs = self.multi_locations[multi_location]
        new_region = Region(region_name, location_desc, mul_locs, storage_price_chf, sku_id)
        self.region_list.append(new_region)
        self.region_by_name[new_region.name] = new_region
        return new_region

    def get_region_obj(self, region):
        region_obj = None
        if isinstance(region, str):
            region_obj = self.region_by_name.get(region)
            if not region_obj:
                raise LookupError('region name {} is not registered'.format(region))
        elif isinstance(region, Region):
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
        elif isinstance(bucket, Bucket):
            bucket_obj = bucket
        else:
            raise TypeError('bucket must be either bucket name or bucket object')
        return bucket

    def create_bucket(self, region, bucket_name, storage_type):
        assert bucket_name not in self.bucket_by_name, bucket_name

        region_obj = self.get_region_obj(region)
        if storage_type == Bucket.TYPE_MULTI:
            region_name = region_obj.name
            if region_name not in ['asia', 'europe', 'us']: # TODO needs better solution!
                raise RuntimeError('create_bucket: cannot create multi regional bucket in region {}'.format(region_name))

        new_bucket = region_obj.create_rse(bucket_name, storage_type)

        self.bucket_list.append(new_bucket)
        self.bucket_by_name[bucket_name] = new_bucket
        return new_bucket
