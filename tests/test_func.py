from .fixtures import cluster_config
from datetime import datetime, timedelta
import pytest
import dwms

TODAY = datetime.now().strftime('%Y%m%d')
YESTERDAY = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


PROGRESS_RESULTS = {
    'sample': {
        'found': {},
        'missing': {},
        'progress': {
            TODAY: 'IN_PROGRESS'
        },
        'partial': {},
        'failed': {}
    }
}

PARTIAL_RESULTS = {
    'sample': {
        'found': {},
        'missing': {},
        'progress': {},
        'partial': {
            TODAY: 'PARTIAL'
        },
        'failed': {}
    }
}

FAILED_RESULTS = {
    'sample': {
        'found': {},
        'missing': {},
        'progress': {},
        'partial': {},
        'failed': {
            TODAY: 'FAILED'
        }
    }
}


MIXED_FAILED = {
    'sample': {
        'found': {},
        'missing': {
            TODAY: 'MISSING'
        },
        'progress': {
            TODAY: 'IN PROGRESS'
        },
        'partial': {
            TODAY: 'PARTIAL'
        },
        'failed': {
            TODAY: 'FAILED'
        }
    }
}


@pytest.mark.parametrize('status,result', [
    (PROGRESS_RESULTS, dwms.Status.IN_PROGRESS),
    (PARTIAL_RESULTS, dwms.Status.PARTIAL),
    (FAILED_RESULTS, dwms.Status.FAILED),
    (MIXED_FAILED, dwms.Status.FAILED)
])
def test_snapshot_statuses(status, result):
    assert dwms.evaluate_snapshots(status) == result
