from setuptools import find_packages, setup
from pip.req import parse_requirements
from pip.download import PipSession
from dwms import __version__ as dwms_version

reqs = parse_requirements('requirements.txt', session=PipSession())
requirements = [str(req.req) for req in reqs]

setup(
    name='dwms',
    author='Casey Weed',
    author_email='casey@caseyweed.com',
    version=dwms_version,
    description='Check if Elasticsearch snapshots are present',
    url='https://github.com/battleroid/dwms',
    py_modules=['dwms'],
    install_requires=requirements,
    tests_require=['pytest'],
    entry_points="""
        [console_scripts]
        dude=dwms:main
    """
)
