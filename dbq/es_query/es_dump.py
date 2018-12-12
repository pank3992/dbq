import sys
import argparse
from runpy import run_path 

from dbq.es_query.client import Client


def main(settings):
    client = Client(hosts=settings['hosts'])

    model = client.get_model(settings['index'])
    model.objects\
        .select(*settings['select'])\
        .get(
            pk_file=settings['pk_file'],
            save_file=settings['save_file'],
            save_format=settings.get('save_format', 'json')
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config')
    args = parser.parse_args()
    settings = run_path(args.config)
    main(settings)
