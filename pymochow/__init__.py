# Copyright 2014 Baidu, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

"""
This module defines some common string constants.
"""
from builtins import str
from builtins import bytes
from . import protocol
from .client.mochow_client import MochowClient

SDK_VERSION = b'1.1.2'
URL_PREFIX = b'/v1'
DEFAULT_ENCODING = 'UTF-8'

__all__ = [
    "MochowClient",
]
