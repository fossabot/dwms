import pytest
from datetime import datetime, timedelta
from dwms import (
    load_config,
    build_cluster_info,
    build_patterns,
    create_clients
)


@pytest.fixture
def entire_config():
    config = load_config('sample.yaml')
    config = build_cluster_info(config)
    config = build_patterns(config)
    config = create_clients(config)
    return config


@pytest.fixture
def cluster_config():
    config = load_config('sample.yaml')
    config = build_cluster_info(config)
    config = build_patterns(config)
    config = create_clients(config)
    return config['clusters'][0]


@pytest.fixture
def broken_cluster_config():
    yesterday = datetime.now() - timedelta(days=1)
    config = load_config('sample.yaml')
    config = build_cluster_info(config)
    config = build_patterns(config, yesterday)
    config = create_clients(config)
    return config['clusters'][0]
