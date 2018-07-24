#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='sparkmeasure',
    version=0.12,
    description='sparkmeasure, Python wrapper part of sparkMeasure, a tool for performance troubleshooting of Apache Spark workloads',
    author='Luca Canali',
    author_email='luca.canali@cern.ch',
    url='https://github.com/lucacanali/sparkMeasure',
    license='Apache v2',
    include_package_data=True,
    package_data={
          'sparkmeasure': ['spark-measure_2.11-0.12-SNAPSHOT.jar'],
    },
    packages=find_packages(),
    zip_safe=False,
    python_requires='>=2.7',
    install_requires=[],
    classifiers=[
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    ]
    )
