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
This module defines exceptions for pymochow.
"""

from pymochow import utils
from pymochow.model.enum import ServerErrCode
from builtins import str
from builtins import bytes

class Error(Exception):
    """Base Error of BCE."""
    def __init__(self, message):
        """
        构造函数
        
        Args:
            message (str): 异常信息
        
        Returns:
            None
        
        """
        Exception.__init__(self, message)


class ClientError(Error):
    """Error from pymochow client."""
    def __init__(self, message):
        """初始化ClientError类及其子类的实例对象。
        
        Args:
            message (str): 错误信息。
        
        """
        Error.__init__(self, message)


class ServerError(Error):
    """Error from mochow servers."""
    REQUEST_EXPIRED = b'RequestExpired'

    """Error threw when connect to server."""
    def __init__(self, message, status_code=None, code=None, request_id=None):
        """
        构造函数，初始化ServerError对象并设置其属性。
        
            Args:
                message (str): 错误描述信息。
                status_code (int, optional): HTTP状态码。默认为None。
                code (str, optional): 请求返回的错误代码。默认为None。
                request_id (str, optional): 请求唯一标识符。默认为None。
        
        """
        Error.__init__(self, message)
        self.status_code = status_code
        self.code = ServerErrCode(code)
        self.request_id = request_id
    

class HttpClientError(Error):
    """Exception threw after retry"""
    def __init__(self, message, last_error):
        """初始化HttpClientError的实例
        
        Args:
            message (str): 错误信息
            last_error (:obj:`Exception`): 上一级的异常对象
        
        """
        Error.__init__(self, message)
        self.last_error = last_error
