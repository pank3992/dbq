import csv
import json
import re
import logging
from datetime import datetime

import aerospike
from elasticsearch_dsl import Q, Search, query
from elasticsearch.helpers import ScanError

from dateutil import parser as date_parser


logger = logging.getLogger('es_query')


class ObjectManager(object):
    max_chunk_size = 2000

    def __init__(self, connection, index):
        self.connection = connection
        self.index = index
        self._select_keys = []
        self._sort_keys = []
        self._filter_kwargs = {}
        self._exclude_kwargs = {}
        self._should_kwargs = {}
        self._request_timeout = 10
        self._save_file = None
        self._save_format = None

    def filter(self, **kwargs):
        self._filter_kwargs = kwargs
        return self

    def exclude(self, **kwargs):
        self._exclude_kwargs = kwargs
        return self

    def should(self, **kwargs):
        self._should_kwargs = kwargs
        return self

    def select(self, *args):
        self._select_keys = args
        return self

    def sort(self, *args):
        # user '-' for descending order, e.g '-abc'
        self._sort_keys = args
        return self

    def get(self, pks=None, pk_file=None, save_file=None, save_format='json', request_timeout=10):

        self._request_timeout = request_timeout
        self._save_file = save_file
        self._save_format = save_format

        if pks and pk_file:
            raise AssertionError('Only one of pks or pk_file is required')

        if self._save_file:
            self._save_records([], False)

        if pks:
            if not isinstance(pks, list):
                raise AssertionError('params pks: should be a list of pks')

            if len(pks) > self.max_chunk_size and self._save_file is None:
                raise AssertionError('for pks greater than {}, \
                    save_file is required'.format(self.max_chunk_size))

            return self._get_from_pks_list(pks)
        elif pk_file:
            return self._get_from_pk_file(pk_file)
        else:
            raise AssertionError('Atleast one of pks or pk_file is required')

    def _get_from_pks_list(self, pks):
        keys = []
        if self._save_file:
            i = 0
            while True:
                pk_chunk = pks[i * self.max_chunk_size :(i + 1) * self.max_chunk_size]
                if not pk_chunk:
                    break
                self._filter_kwargs['_id__in'] = pk_chunk
                self._scan_wrapper(self.max_chunk_size, True)
                i += 1
            resp = None
        else:
            self._filter_kwargs['_id__in'] = pks
            resp = self._scan_wrapper(self.max_chunk_size, True)
        return resp

    def _get_from_pk_file(self, pk_file):

        if not self._save_file:
            raise AssertionError('save_file param is required with pk_file param')
        else:
            pks = []
            with open(pk_file) as f:
                for line in f.readlines():
                    # Accepted pk format: any string with ['alphabets', '.', '@', '-']
                    for pk in re.findall(r'[\w@.-]+', line):
                        pks.append(pk)
                        if len(pks) == self.max_chunk_size:
                            self._get_from_pks_list(pks)
                            pks = []

                # write any left overs
                if pks:
                    self._get_from_pks_list(pks)

    def scan(self, max_records_count=20, save_file=None, save_format='json', request_timeout=10, clear_scroll=True):
        self._request_timeout = request_timeout
        self._save_file = save_file
        self._save_format = save_format

        if (max_records_count > self.max_chunk_size or max_records_count == -1) and not self._save_file:
            raise AssertionError('save_file is required for \
                max_records_count greater than {}'.format(self.max_chunk_size))
        elif self._save_file:
            self._save_records([], False)

        return self._scan_wrapper(max_records_count, clear_scroll)

    def _scan_wrapper(self, max_records_count, clear_scroll=True):
        search = self._get_search_obj(max_records_count)

        if max_records_count > self.max_chunk_size or max_records_count == -1:
            try:
                records = []
                count = 0
                search = search.params(
                    size=self.max_chunk_size,
                    clear_scroll=clear_scroll,
                    scroll='5m'
                )
                for hit in search.scan():
                    records.append(self._parse_record(hit))

                    if self._save_file and len(records) == self.max_chunk_size:
                        self._save_records(records)
                        records[:] = []

                    count += 1
                    if max_records_count != -1 and count > max_records_count:
                        break

                self._save_records(records)
                resp = None
            except ScanError as error:
                logger.exception(error)
                raise error
        else:
            results = search.execute()
            resp = []
            for hit in results.hits:
                resp.append(self._parse_record(hit))
            if self._save_file:
                self._save_records(resp)
        return resp

    def count(self):
        search = self._get_search_obj(0)
        result = search.execute()
        return result.hits.total

    def _get_search_obj(self, size=0):
        _query = query.Bool(
            must = self._build_query(self._filter_kwargs),
            must_not = self._build_query(self._exclude_kwargs),
            should = self._build_query(self._should_kwargs)
        )

        search = Search(
            using = self.connection,
            index = self.index
        )\
        .query(_query)\
        .source(self._select_keys)\
        .extra(size=size)\
        .params(request_timeout=self._request_timeout)\
        .sort(*self._sort_keys)

        logger.info(json.dumps(result.to_dict()))
        return search

    def _save_records(self, records, append_mode=True):

        write_mode = 'a' if append_mode else 'w'
        with open(self._save_file, write_mode) as f:
            if self._save_format == 'json':
                for record in records:
                    f.writelines([json.dumps(record), '\n'])
            elif self._save_format == 'csv':
                if not self._select_keys:
                    raise AssertionError('select attribute is required for csv dump')

                writer = csv.DictWriter(f, self._select_keys)

                # write headers only in case of new file
                if not append_mode:
                    writer.writeheader()
                writer.writerows(records)
            else:
                raise AssertionError('Invalid file save_format: {}'.format(self._save_format))

    def _build_query(self, filter_kwargs):

        query = []

        for filter_key, filter_value in filter_kwargs.items():

            filter_key = filter_key.split('__')

            if filter_key[-1] in ['ne', 'in', 'iexact', 'contains', 'gte', 'gt', 'lte', 'lt']:
                filter_type = filter_key[-1]
                filter_key = '__'.join(filter_key[:-1])
            else:
                filter_type = None
                filter_key = '__'.join(filter_key)

            if filter_type is None:
                if filter_value is None:
                    query.append(~Q('exists', field=filter_key))
                else:
                    query.append(Q('term', ** {filter_key: filter_value}))
            elif filter_type == 'ne':
                if filter_value is None:
                    query.append(Q('exists', field=filter_key))
                else:
                    query.append(~Q('term', ** {filter_key: filter_value}))
            elif filter_type == 'in':
                if None in filter_value:
                    filter_value.remove(None)
                    query.append(Q('bool', should=[
                        ~Q('exists', field=filter_key),
                        Q('terms', ** {filter_key: filter_value})
                    ]))
                else:
                    query.append(Q('terms', ** {filter_key: filter_value}))
            elif filter_type == 'iexact':
                query.append(Q('match', ** {filter_key: filter_value}))
            elif filter_type == 'contains':
                query.append(Q('wildcard', ** {filter_key: '*{}*'.format(filter_value)}))
            elif filter_type == 'gte':
                query.append(Q('range', ** {filter_key: {'gte': filter_value}}))
            elif filter_type == 'gt':
                query.append(Q('range', ** {filter_key: {'gt': filter_value}}))
            elif filter_type == 'lte':
                query.append(Q('range', ** {filter_key: {'lte': filter_value}}))
            elif filter_type == 'lt':
                query.append(Q('range', ** {filter_key: {'lt': filter_value}}))
            else:
                raise AssertionError('Invalid filter: {}'.format('__'.join(filter_key)))

        return query

    def _parse_record(self, record):
        # converting records to dict format from flat structure
        if not self._select_keys:
            resp = record.to_dict()
            resp['pk'] = record.meta['id']
        else:
            resp = {}
            record_dict = record.to_dict()
            for key in self._select_keys:
                if key == 'pk':
                    resp['pk'] = record.meta['id']
                    continue
                key_split = key.split('__')
                value = record_dict.get(key_split[0])
                for split in key_split[1:]:
                    if isinstance(value, dict):
                        value = value.get(split)
                resp[key] = value
        return resp

class ObjectModel(object):

    def __init__(self, connection, index):
        self.connection = connection
        self.index = index

    @property
    def objects(self):
        return ObjectManager(self.connection, self.index)
