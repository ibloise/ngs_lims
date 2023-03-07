from setuptools import find_packages
from setuptools import setup

setup(
    name = 'NGS_LIMS',
    version = '0.1.0',
    author = 'Iván Bloise Sánchez',
    description= 'Functions and other tools for Opentrons protocols',
    packages={'ngs_lims': "ngs_lims"},
    entry_points = {
    'console_scripts': [
    'lims_get_fastq=ngs_lims.get_fastq:main'
    ]
    }
)