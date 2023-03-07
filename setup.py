from setuptools import find_packages
from setuptools import setup

setup(
    name = 'NGS_LIMS',
    version = '0.1.0',
    author = 'Iván Bloise Sánchez',
    description= 'Minimal LIMS for NGS data and files',
    packages=find_packages(),
    entry_points = {
    'console_scripts': [
    'lims_get_fastq=ngs_lims.get_fastq:main'
    ]
    }
)