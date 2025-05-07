#!/usr/bin/env python

from setuptools import setup, find_packages

long_description = '''
SparkMeasure is a tool for performance troubleshooting of Apache Spark workloads.
It simplifies the collection and analysis of Spark performance metrics. The bulk of sparkMeasure is written in Scala.
This package contains the Python API for sparkMeasure.
Use it from Python programs, PySpark, Jupyter notebooks, or in general as a tool to instrument and collect runtime metrics for your Spark jobs.

Key Features:
- Interactive troubleshooting for Spark workloads.
- Batch job analysis with Flight Recorder mode.
- Monitoring with external systems (InfluxDB, Kafka, Prometheus).
- Language support for Scala, Java, and Python.
- Educational tool demonstrating Spark Listener interface.

For more information, see the [sparkMeasure GitHub page and documentation](https://github.com/lucacanali/sparkMeasure).
'''

setup(
    name='sparkmeasure',
    version='0.25.0',
    description='Python API for sparkMeasure, a tool for performance troubleshooting of Apache Spark workloads.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Luca Canali',
    author_email='luca.canali@cern.ch',
    url='https://github.com/lucacanali/sparkMeasure',
    license='Apache License 2.0',
    include_package_data=True,
    packages=find_packages(include=['sparkmeasure', 'sparkmeasure.*']),
    zip_safe=False,
    python_requires='>=3.9',
    install_requires=[
        'pyspark>=3.0.0',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: Apache Software License',
    ],
    keywords='spark, apache spark, performance, monitoring, metrics, troubleshooting',
    project_urls={
        'Documentation': 'https://github.com/lucacanali/sparkMeasure',
        'Source': 'https://github.com/lucacanali/sparkMeasure',
        'Tracker': 'https://github.com/lucacanali/sparkMeasure/issues',
    },
)
