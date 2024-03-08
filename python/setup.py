#!/usr/bin/env python

from setuptools import setup, find_packages

description = 'Python API for sparkMeasure, a tool for performance troubleshooting of Apache Spark workloads.'

long_description = """SparkMeasure is a tool for performance troubleshooting of Apache Spark workloads.  
It simplifies the collection and analysis of Spark performance metrics. The bulk of sparkMeasure is written in Scala.
This package contains the Python API for sparkMeasure.
Use it from Python programs, PySpark, Jupyter notebooks, or in general as a tool to instrument and collect runtime metrics for your Spark jobs.  
**[Link to sparkMeasure GitHub page and documentation](https://github.com/lucacanali/sparkMeasure)**"""

setup(name='sparkmeasure',
    version='0.24.0',
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Luca Canali',
    author_email='luca.canali@cern.ch',
    url='https://github.com/lucacanali/sparkMeasure',
    license='Apache License, Version 2.0',
    include_package_data=True,
    packages=find_packages(),
    zip_safe=False,
    python_requires='>=3.8',
    install_requires=[],
    classifiers=[
    'Programming Language :: Python :: 3',
    'Operating System :: OS Independent',
    'License :: OSI Approved :: Apache Software License',
    'Intended Audience :: Developers',
    'Development Status :: 4 - Beta',
    ]
    )
