import csv
import json
import re
import logging
from datetime import datetime
from math import ceil

import aerospike
from dateutil import parser as date_parser


logger = logging.getLogger('as_query')


class ObjectManager(object):
    def __init__(self, connection, namespace, set, scan_options=None):
        self.namespace = namespace
        self.set = set
        self.connection = connection
        self._select_keys = []
        self._filter_kwargs = {}
        self._exclude_kwargs = {}
        self.scan_options = {
            'concurrent': True,
            'priority': aerospike.SCAN_PRIORITY_LOW
        }
        self.get_policy = {
            'max_retries': 3,
            'total_timeout': 3000,
            'concurrent': True,
            'sleep_between_retries': 200
        }

    def filter(self, **kwargs):
        self._filter_kwargs = kwargs
        return self

    def exclude(self, **kwargs):
        self._exclude_kwargs = kwargs
        return self

    def select(self, *args, **kwargs):

        if 'pk' in args:
            logger.warning('PK will be fetched iff the record was saved with SEND_KEY=true policy')

        self._select_keys = args
        return self

    def get(self, pks=None, pk_file=None, save_file=None, save_format='json', batch_size=5000):
        self._save_file = save_file
        self._save_format = save_format
        self._batch_size = batch_size

        if pks and pk_file:
            raise AssertionError('Only one of pks or pk_file is required')

        if self._save_file:
            self._save_records([], False)

        if pks:
            return self._get_from_pks_list(pks)
        elif pk_file:
            return self._get_from_pk_file(pk_file)
        else:
            raise AssertionError('Atleast one of pks or pk_file is required')

    def _get_from_pks_list(self, pks, append_mode=False):
        if not isinstance(pks, list):
            raise AssertionError('params pks: should be a list of pks')

        records = []

        if len(pks) > self._batch_size:
            i = 0
            while True:
                chunk = pks[i * self._batch_size:(i + 1) * self._batch_size]
                if not chunk:
                    break
                records += self._get_from_pks_list(chunk, append_mode)
                i += 1
            return records

        keys = []
        for each in pks:
            keys.append((self.namespace, self.set, each))

        query_bins = self._query_bins()
        if query_bins:
            records = self.connection.select_many(keys, list(query_bins), self.get_policy)
        else:
            records = self.connection.get_many(keys, self.get_policy)

        filtered_records = []
        for r in records:
            if self._is_valid_record(r):
                bins = self._get_bins(self._select_keys, r)
                filtered_records.append(bins)

        if self._save_file:
            self._save_records(filtered_records, append_mode)
        return filtered_records

    def _get_from_pk_file(self, pk_file):

        if not self._save_file:
            raise AssertionError('save_file param is required')
        else:
            with open(pk_file) as f:
                pks = []
                for line in f.readlines():
                    # Accepted pk format: any string with ['alphabets', '.', '@', '-']
                    for pk in re.findall(r'[\w@.-]+', line):
                        pks.append(pk)
                        if len(pks) > self._batch_size:
                            self._get_from_pks_list(pks, True)
                            pks = []

                # write any left overs
                if pks:
                    self._get_from_pks_list(pks, True)

    def scan(self, max_records_count=20, max_scans_count=100000, save_file=None, save_format='json', scan_option=None):

        self._save_file = save_file
        self._save_format = save_format

        if (max_scans_count == -1 or max_records_count > 100000) and not self._save_file:
            raise AssertionError('Save file is required for max_records_count greater than 100000')

        if self._save_file:
            self._save_records([], False)

        scanner = self.connection.scan(self.namespace, self.set)

        query_bins = self._query_bins()
        if query_bins:
            scanner.select(*list(query_bins))

        scan_option = scan_option or self.scan_options

        filtered_records = []
        scanner.foreach(
            self._scan_callback(filtered_records, max_records_count, max_scans_count),
            options=scan_option
        )

        if self._save_file and filtered_records:
            # As _scan_callback saves in batches, there can few filtered result
            # which are not saved to file
            self._save_records(filtered_records, True)
            filtered_records = None
        return filtered_records

    def _save_records(self, records, append_mode):
        write_mode = 'a' if append_mode else 'w'
        with open(self._save_file, write_mode) as f:
            if self._save_format == 'json':
                for each in records:
                    f.writelines([json.dumps(each), '\n'])
            elif self._save_format == 'csv':
                if not self._select_keys:
                    raise AssertionError('select attribute is required for csv dump')

                writer = csv.DictWriter(f, self._select_keys)

                # write headers only in case of new file
                if not append_mode:
                    writer.writeheader()

                # converting records to dict format from flat structure
                if len(self._select_keys) == 1:
                    records = [{self._select_keys[0]: e} for e in records]

                writer.writerows(records)
            else:
                raise AssertionError('Invalid file save_format: {}'.format(self._save_format))

    def _is_valid_record(self, record):
        if not record[2]:
            return False

        filter_success = True
        if self._filter_kwargs:
            filter_success = self._is_valid_record_wrapper(self._filter_kwargs, record[2])

        exclude_success = True
        if self._exclude_kwargs:
            exclude_success = not self._is_valid_record_wrapper(self._exclude_kwargs, record[2])

        return True if filter_success and exclude_success else False

    def _is_valid_record_wrapper(self, filter_kwargs, record):
        is_valid = True

        for filter_key, filter_value in filter_kwargs.items():
            is_valid = False

            filter_key = filter_key.split('__')
            filter_key_len = len(filter_key)

            value = record.get(filter_key[0])

            if isinstance(value, str) and isinstance(filter_value, datetime):
                if value.isdigit():
                    value = value[0:14]  # truncating milliseconds
                value = date_parser.parse(value)

            if filter_key_len == 1:
                if callable(filter_value):
                    is_valid = filter_value(value)
                    assert type(is_valid) == bool, '{} should return a boolean'.format(
                        filter_key)
                elif filter_value == value:
                    is_valid = True
            else:
                if filter_key[1] == 'ne':
                    if value != filter_value:
                        is_valid = True
                elif filter_key[1] == 'in':
                    if value in filter_value:
                        is_valid = True
                elif filter_key[1] == 'iexact':
                    if isinstance(value, str) and value.lower() == filter_value.lower():
                        is_valid = True
                elif filter_key[1] == 'contains':
                    if isinstance(value, str) and value.count(filter_value):
                        is_valid = True
                elif filter_key[1] == 'icontains':
                    if isinstance(value, str) and value.lower().count(filter_value.lower()):
                        is_valid = True
                elif filter_key[1] == 'gte':
                    if isinstance(value, (int, str, datetime)) and value >= filter_value:
                        is_valid = True
                elif filter_key[1] == 'gt':
                    if isinstance(value, (int, str, datetime)) and value > filter_value:
                        is_valid = True
                elif filter_key[1] == 'lte':
                    if isinstance(value, (int, str, datetime)) and value <= filter_value:
                        is_valid = True
                elif filter_key[1] == 'lt':
                    if isinstance(value, (int, str, datetime)) and value < filter_value:
                        is_valid = True
                elif isinstance(value, dict):
                    nested_filter_key = '__'.join(filter_key[1:])
                    nested_filter = {
                        nested_filter_key: filter_value
                    }
                    is_valid = self._is_valid_record(nested_filter, value)
                else:
                    raise AssertionError('Invalid filter: {}'.format('__'.join(filter_key)))

            if not is_valid:
                break

        return is_valid

    def _scan_callback(self, records, max_records_count=20, max_scans_count=100000):
        # integers are immutable so a list (mutable) is used for the counter
        total_records = self.get_total_objects()
        current_count = [0, 0]

        def wrapper(record):
            try:
                if max_scans_count != -1 and current_count[0] >= max_scans_count:
                    return False
                elif max_records_count != -1 and current_count[1] >= max_records_count:
                    return False

                if self._is_valid_record(record):
                    bins = self._get_bins(self._select_keys, record)
                    records.append(bins)
                    current_count[1] += 1

                current_count[0] += 1
                if current_count[0] % 50000 == 0:
                    logger.info('Total Records: %s/rf, Total Scanned: %s, Records Found: %s', total_records, current_count[0], current_count[1])
                    if self._save_file:
                        self._save_records(records, True)
                        records[:] = []  # clear
            except Exception as e:
                logger.exception('Unable to filter record')
                raise e

            return records

        return wrapper

    def get_total_objects(self):
        clusters_info = self.connection.info('sets')

        objects_count = 0
        regex = 'ns={}:set={}:objects=(\d+)?:'.format(self.namespace, self.set)

        for cluster_id, info in clusters_info.items():
            count = re.findall(regex, info[1])
            if count:
                objects_count += int(count[0])

        if objects_count == 0:
            raise AssertionError('Invalid namespace-set or no records found')
        return objects_count

    def _get_bins(self, select_keys, record):

        if not select_keys:
            return record[2]

        bins = {}
        if 'pk' in select_keys:
            bins['pk'] = record[0][3]

        for key in select_keys:
            key_split = key.split('__')
            value = record[2].get(key_split[0])

            if len(key_split) == 1 or not value:
                bins[key] = value
            elif len(key_split) > 1 and isinstance(value, dict):
                bins[key] = self._get_bins(['__'.join(key_split[1:])], [None , None, value])
            else:
                raise AssertionError('Invalid bin type: {}'.format(key))

        if len(select_keys) == 1:
            return bins[select_keys[0]]
        return bins

    def _query_bins(self):
        query_bins = None

        if self._select_keys:
            query_bins = set(self._select_keys)

            for each in list(self._filter_kwargs.keys()) + list(self._exclude_kwargs.keys()):
                query_bins.add(each.split('__')[0])
        return query_bins


class ObjectModel(object):

    def __init__(self, connection, namespace, set):
        self.connection = connection
        self.namespace = namespace
        self.set = set

    @property
    def objects(self):
        return ObjectManager(self.connection, self.namespace, self.set)
