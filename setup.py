#!/usr/bin/env python
# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2023 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
Setup script.

Authors: guobo(guobo@baidu.com)
Date:    2023/11/17 11:01:15
"""
import io
import os
import re
from setuptools import setup

with io.open(os.path.join("pymochow", "__init__.py"), "rt", encoding='utf-8') as f:
    SDK_VERSION = re.search(r"SDK_VERSION = b'(.*?)'", f.read()).group(1)

setup(
    name='pymochow',
    version=SDK_VERSION,
    install_requires=[
        'requests',
        'orjson',
        'future'
    ],
    python_requires='>=3.7',
    packages=[
        'pymochow',
        'pymochow.auth',
        'pymochow.http',
        'pymochow.retry',
        'pymochow.client',
        'pymochow.model',
        'pymochow.ai',
        'pymochow.ai.dochub',
        'pymochow.ai.parser',
        'pymochow.ai.splitter',
        'pymochow.ai.processor',
        'pymochow.ai.embedder',
        'pymochow.ai.pipeline'
    ],
    url='http://bce.baidu.com',
    license='Apache License 2.0',
    author='fengjialin',
    author_email='fengjialin@baidu.com',
    description='Python SDK for mochow'
)
