from .fixtures import entire_config, cluster_config, broken_cluster_config
from datetime import datetime
import dwms


def test_config(entire_config, cluster_config):
    """Ensure cluster gets global settings set"""
    assert entire_config['settings'] == cluster_config['settings']


def test_credentials(cluster_config):
    """Ensure credential check works"""
    assert dwms.check_credentials(cluster_config)


def test_check_snapshots(cluster_config):
    """Ensure snapshot for 'today' is found"""
    now = datetime.now().strftime('%Y%m%d')
    status = dwms.check_snapshots(cluster_config)
    assert now in status['sample']['found']


def test_good_evaluation(cluster_config):
    """Ensure the correct evaluation is set"""
    status = dwms.check_snapshots(cluster_config)
    status = dwms.evaluate_snapshots(status)
    assert status == dwms.Status.OKAY


def test_bad_evaluation(broken_cluster_config):
    """Ensure the correct evaluation is set"""
    status = dwms.check_snapshots(broken_cluster_config)
    status = dwms.evaluate_snapshots(status)
    assert status == dwms.Status.MISSING
