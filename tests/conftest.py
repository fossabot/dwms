from elasticsearch import Elasticsearch
from subprocess import run
from datetime import datetime
import pytest
import time

def pytest_configure(config):
    run('docker-compose up -d'.split())

    # give it some time to startup
    time.sleep(20)

    # create our repo
    es = Elasticsearch('localhost:9200')
    es.snapshot.create_repository(
        repository='sample',
        body={
            'type': 'fs',
            'settings': {
                'location': '/tmp/backup',
                'compress': False
            }
        },
        verify=True
    )

    # index a couple bogus docs for backups
    for x in range(10):
        es.index(
            index='sample',
            doc_type='logs',
            body={'field': f'Hello World #{x}!'}
        )

    # back them up, can't lose those really import greetings
    es.snapshot.create(
        repository='sample',
        snapshot=datetime.now().strftime('%Y%m%d'),
        wait_for_completion=True
    )


def pytest_unconfigure(config):
    run('docker-compose down'.split())
    run('docker-compose rm -f -v'.split())
