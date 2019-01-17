from elasticsearch import Elasticsearch

from dbq.es_query.model import ObjectModel

from dbq.utils import create_logger


class Client(object):

    def __init__(self, hosts, log_path=None, **kwargs):

        log_path = log_path or '/var/log/dbq/es_query'
        create_logger('es_query', log_path)
        self.connection = Elasticsearch(hosts=hosts, **kwargs)

    def get_model(self, index):
        return ObjectModel(self.connection, index)
