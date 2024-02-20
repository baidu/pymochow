# Copyright 2023 Baidu, Inc.
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
This module provides a general response class for mochow services.
"""
from future.utils import iteritems
from builtins import str
from builtins import bytes
from pymochow import utils
from pymochow import compat
from pymochow.http import http_headers


class HttpResponse(object):
    """
    
    :param object:
    :return:
    """
    def __init__(self):
        """初始化Metadata对象
        
        Args:
            无参数
        
        Returns:
            None
        """
        self.metadata = utils.Expando()

    def set_metadata_from_headers(self, headers):
        """

        :param headers:
        :return:
        """
        for k, v in iteritems(headers):
            if k.startswith(compat.convert_to_string(http_headers.BCE_PREFIX)):
                k = 'bce_' + k[len(compat.convert_to_string(http_headers.BCE_PREFIX)):]
            k = utils.pythonize_name(k.replace('-', '_'))
            if k.lower() == compat.convert_to_string(http_headers.ETAG.lower()):
                v = v.strip('"')
            setattr(self.metadata, k, v)

    def __getattr__(self, item):
        """
        重写基类中 __getattr__ 方法，当属性名前缀为 '__' 时抛出 AttributeError 异常。
        
        Args:
            self (object): 对象指针。
            item (str): 属性名。
        
        Returns:
            Optional[Any]: 返回值为None或属性值。
        
        Raises:
            AttributeError: 当属性名前缀为 '__' 时抛出该异常。
        """
        if item.startswith('__'):
            raise AttributeError
        return None

    def __repr__(self):
        """
        返回一个用于打印的字符串，包含对象的属性和值。
        
        Args:
            无参数。
        
        Returns:
            str: 包含对象的属性和值的字符串。
        
        """
        return utils.print_object(self)
