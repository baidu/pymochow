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
This module provide http request function for bce services.
"""
from future.utils import iteritems, iterkeys, itervalues
from builtins import str, bytes
import logging
import sys
import time
import traceback
import platform
import requests
from requests.adapters import HTTPAdapter
from requests.adapters import PoolManager
import socket
from urllib3.connection import HTTPConnection

import pymochow
from pymochow import compat
from pymochow import utils
from pymochow.http.http_response import HttpResponse
from pymochow.exception import HttpClientError
from pymochow.exception import ClientError
from pymochow.http import http_headers
from pymochow.http import http_methods
from pymochow.http import handler
from pymochow.auth import bce_v1_signer

_logger = logging.getLogger(__name__)


class _SockOpsAdapter(HTTPAdapter):
    def __init__(self, options, **kwargs):
        self.options = options
        super(_SockOpsAdapter, self).__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        """init connection pool"""
        self.poolmanager = PoolManager(num_pools=connections,
                                       maxsize=maxsize,
                                       block=block,
                                       socket_options=self.options)


class HTTPClient:
    """http client"""

    def __init__(self, config, adapter: HTTPAdapter = None):
        """create http client"""
        self.session = requests.Session()
        self._set_adapter(adapter)
    
    def _set_adapter(self, adapter: HTTPAdapter = None):
        """set http adapter"""
        if not adapter:
            if 'linux' not in platform.platform().lower():
                return
            options = HTTPConnection.default_socket_options + [
                (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
                (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 120),
                (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10),
                (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3),
            ]
            adapter = _SockOpsAdapter(pool_connections=10,
                                      pool_maxsize=10, max_retries=3, options=options)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
    
    def check_headers(self, headers):
        """
        check value in headers, if \n in value, raise
        :param headers:
        :return:
        """
        for k, v in iteritems(headers):
            if isinstance(v, (bytes, str)) and \
            b'\n' in compat.convert_to_bytes(v):
                raise ClientError(r'There should not be any "\n" in header[%s]:%s' % (k, v))

    def send_request(self, 
            http_method, 
            path=None, 
            body=None, 
            headers=None, 
            params=None,
            config=None,
            body_parser=None,
            ):
        """send http request
        Args:
            http_method (str): http method
            path (Optional[str]): http uri
            body (Any): http body
            headers (Dict[str, str]): http headers
            params (Dict[str, Any]): http params
            config (Optional[Configuration]): client configuration
            body_parser (Optional[Callable[[bytes], object]]): http body parser
        Returns:
            Union[object, Tuple[object, int]]: 当body_parser为None时返回结果对象，当body_parser不为None时返回元组包含结果对象和状态码
        Raises:
            HttpClientError: Http客户端异常
            ClientException: 客户端异常
            ValueError: 参数错误
        """
        if body_parser is None:
            body_parser = handler.parse_json
       
        try:
            return self._send_request(
                    config, bce_v1_signer.sign, 
                    [handler.parse_error, body_parser],
                    http_method, path, body, headers, params)
        except Exception as e:
            raise e
    
    def _send_request(self, 
            config, 
            sign_function, 
            response_handler_functions,
            http_method,
            path,
            body,
            headers,
            params):
        """send http request
        Args:
            config (Optional[Configuration]): client configuration
            sign_function(Optional[Callable]): sign function
            response_handler_functions(List[Callable]): response handler functions
            http_method (str): http method
            path (Optional[str]): http uri
            body (Any): http body
            headers (Dict[str, str]): http headers
            params (Dict[str, Any]): http params
        """
        _logger.debug(b'%s request start: %s %s, %s',
                      http_method, path, headers, params)
        headers = headers or {}

        user_agent = 'pymochow/%s/%s/%s' % (
            compat.convert_to_string(pymochow.SDK_VERSION), sys.version, sys.platform)
        user_agent = user_agent.replace('\n', '')
        user_agent = compat.convert_to_bytes(user_agent)
        headers[http_headers.USER_AGENT] = user_agent
     
        should_get_new_date = False
        if http_headers.BCE_DATE not in headers:
            should_get_new_date = True

        request_endpoint = config.endpoint

        headers[http_headers.HOST] = request_endpoint

        if isinstance(body, str):
            body = body.encode(pymochow.DEFAULT_ENCODING)
        if not body:
            headers[http_headers.CONTENT_LENGTH] = '0'
        elif isinstance(body, bytes):
            headers[http_headers.CONTENT_LENGTH] = str(len(body))
        elif http_headers.CONTENT_LENGTH not in headers:
            raise ValueError(b'No %s is specified.' % http_headers.CONTENT_LENGTH)
        # store the offset of fp body
        offset = None
        if hasattr(body, "tell") and hasattr(body, "seek"):
            offset = body.tell()

        protocol, host, port = utils.parse_host_port(request_endpoint, config.protocol)
        url = request_endpoint + path
        
        headers[http_headers.HOST] = host
        if port != config.protocol.default_port:
            headers[http_headers.HOST] += b':' + compat.convert_to_bytes(port)

        headers[http_headers.AUTHORIZATION] = sign_function(
            config.credentials, http_method, path, headers, params)

        encoded_params = utils.get_canonical_querystring(params, False)
        if len(encoded_params) > 0:
            uri = path + b'?' + encoded_params
        else:
            uri = path
        self.check_headers(headers)

        retries_attempted = 0
        errors = []
        while True:
            try:
                # restore the offset of fp body when retrying
                if should_get_new_date is True:
                    headers[http_headers.BCE_DATE] = utils.get_canonical_time()

                headers[http_headers.AUTHORIZATION] = sign_function(
                    config.credentials, http_method, path, headers, params)

                if retries_attempted > 0 and offset is not None:
                    body.seek(offset)
                if http_method == http_methods.POST:
                    http_response = self.session.post(url, data=body,
                            params=params,
                            headers=headers,
                            timeout=config.connection_timeout_in_mills)
                elif http_method == http_methods.DELETE:
                    http_response = self.session.delete(url, data=body,
                            params=params,
                            headers=headers,
                            timeout=config.connection_timeout_in_mills)
                else:
                    raise ClientError(message="Http method {} not supported.".format(http_method))
                _logger.debug('request args:method=%s, uri=%s, headers=%s, patams=%s, body=%s',
                        http_method, uri, headers, params, body)

                headers_list = http_response.headers

                # on py3 ,values of headers_list is decoded with ios-8859-1 from
                # utf-8 binary bytes

                # headers_list[*][0] is lowercase on py2
                # headers_list[*][0] is raw value py3
                if compat.PY3 and isinstance(headers_list, list):
                    temp_heads = []
                    for k, v in headers_list:
                        k = k.encode('latin-1').decode('utf-8')
                        v = v.encode('latin-1').decode('utf-8')
                        k = k.lower()
                        temp_heads.append((k, v))
                    headers_list = temp_heads

                _logger.debug(
                    'request return: status=%d, headers=%s' %
                    (http_response.status_code, headers_list))
                response = HttpResponse()
                response.set_metadata_from_headers(dict(headers_list))

                for handler_function in response_handler_functions:
                    if handler_function(http_response, response):
                        break
                _logger.debug('response:%s' % (response))
                return response
            except Exception as e:
                # insert ">>>>" before all trace back lines and then save it
                errors.append('\n'.join('>>>>' + line for line in traceback.format_exc().splitlines()))

                if config.retry_policy.should_retry(e, retries_attempted):
                    delay_in_millis = config.retry_policy.get_delay_before_next_retry_in_millis(
                        e, retries_attempted)
                    time.sleep(delay_in_millis / 1000.0)
                else:
                    _logger.debug('Unable to execute HTTP request. Retried %d times. '
                            'All trace backs:\n%s' % (retries_attempted,
                            '\n'.join(errors)))
                    raise e
                    #raise HttpClientError('Unable to execute HTTP request. Retried %d times. '
                    #        'All trace backs:\n%s' % (retries_attempted,
                    #        '\n'.join(errors)), e)

            retries_attempted += 1
        

    def close(self):
        """close session"""
        self.session.close()
