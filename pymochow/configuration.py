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
This module defines a common configuration class for BCE.
"""

from future.utils import iteritems
from builtins import str
from builtins import bytes
import pymochow.protocol
from pymochow.retry.retry_policy import BackOffRetryPolicy
from pymochow import compat


class Configuration(object):
    """Configuration of Bce client."""

    def __init__(self,
                 credentials=None,
                 endpoint=None,
                 protocol=None,
                 connection_timeout_in_mills=None,
                 send_buf_size=None,
                 recv_buf_size=None,
                 retry_policy=None,
                 security_token=None,
                 cname_enabled=False,
                 backup_endpoint=None,
                 proxy_host=None,
                 proxy_port=None,):
        """初始化方法，用于创建 OSS Client 实例。
        
        Args:
            credentials (dict): 包含 AccessKeyID 和 SecretAccessKey 的字典。
            endpoint (str): 请求的 OSS 服务端点 URL 。
            protocol (str): HTTP 协议名称（默认值：http） 
            connection_timeout_in_mills (int): 连接超时时间，单位为毫秒（默认值：3000ms）。
            send_buf_size (int): 发送缓冲区大小，单位字节（默认值：256KB）。
            recv_buf_size (int): 接收缓冲区大小，单位字节（默认值：1MB）。
            retry_policy (:class:`alibabacloud.ossutil.client.BackOffRetryPolicy`): 重试策略对象，用于设置失败请求的重试次数和重试间隔时间。
            security_token (str): SecurityToken ，阿里云身份验证安全令牌。
            cname_enabled (bool): 是否启用 CNAME 访问模式（默认值：False）。
            backup_endpoint (str): 备用 OSS 服务端点 URL。
            proxy_host (str): http/https 代理主机地址。
            proxy_port (int): http/https 代理端口号。
        
        Returns:
            :class:`alibabacloud.ossutil.client.Client`
        """
        self.credentials = credentials
        self.endpoint = compat.convert_to_bytes(endpoint) if endpoint is not None else endpoint
        self.protocol = protocol
        self.connection_timeout_in_mills = connection_timeout_in_mills
        self.send_buf_size = send_buf_size
        self.recv_buf_size = recv_buf_size
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        if retry_policy is None:
            self.retry_policy = BackOffRetryPolicy()
        else:
            self.retry_policy = retry_policy
        self.security_token = security_token
        self.cname_enabled = cname_enabled
        self.backup_endpoint = compat.convert_to_bytes(backup_endpoint) \
                if backup_endpoint is not None else backup_endpoint

    def merge_non_none_values(self, other):
        """

        :param other:
        :return:
        """
        for k, v in iteritems(other.__dict__):
            if v is not None:
                self.__dict__[k] = v


DEFAULT_PROTOCOL = pymochow.protocol.HTTP
DEFAULT_CONNECTION_TIMEOUT_IN_MILLIS = 50 * 1000
DEFAULT_SEND_BUF_SIZE = 1024 * 1024
DEFAULT_RECV_BUF_SIZE = 10 * 1024 * 1024
DEFAULT_CONFIG = Configuration(
    protocol=DEFAULT_PROTOCOL,
    connection_timeout_in_mills=DEFAULT_CONNECTION_TIMEOUT_IN_MILLIS,
    send_buf_size=DEFAULT_SEND_BUF_SIZE,
    recv_buf_size=DEFAULT_RECV_BUF_SIZE,
    retry_policy=BackOffRetryPolicy())
