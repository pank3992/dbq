import aerospike

from dbq.utils import create_logger

from dbq.as_query.model import ObjectModel


class Client(object):

    def __init__(self, hosts, log_path=None, **kwargs):

        log_path = log_path or '/var/log/dbq/as_query'
        logger = create_logger('as_query', log_path)

        try:
            self._connection = aerospike.client({
                'hosts': hosts
            })

            self._connection.connect()
            logger.info('Successfully connected with Aerospike Cluster')
        except Exception as e:
            logger.exception("Failed to connect with Aerospike Cluster")
            raise e

    def get_model(self, namespace, set):
        return ObjectModel(self._connection, namespace, set)
