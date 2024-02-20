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
This module provides general http handler functions for processing http responses from BCE services.
"""

import http.client
from builtins import str
from builtins import bytes
import orjson
from pymochow import utils
from pymochow import compat
from pymochow.exception import ClientError
from pymochow.exception import ServerError

def parse_json(http_response, response):
    """If the body is not empty, convert it to a python object and set as the value of
    response.body. http_response is always closed if no error occurs.

    :param http_response: the http_response object returned by HTTPConnection.getresponse()
    :type http_response: httplib.HTTPResponse

    :param response: general response object which will be returned to the caller
    :type response: pymochow.http.HttpResponse

    :return: always true
    :rtype bool
    """
    body = http_response.text
    if body:
        body = compat.convert_to_string(body)
        #response.__dict__.update(json.loads(body, object_hook=utils.dict_to_python_object).__dict__)
        data = orjson.loads(body)
        python_object = utils.dict_to_python_object(data)
        response.__dict__.update(python_object.__dict__)
        response.__dict__["raw_data"] = body
    http_response.close()
    return True


def parse_error(http_response, response):
    """If the body is not empty, convert it to a python object and set as the value of
    response.body. http_response is always closed if no error occurs.

    :param http_response: the http_response object returned by HTTPConnection.getresponse()
    :type http_response: httplib.http.HTTPResponse

    :param response: general response object which will be returned to the caller
    :type response: pymochow.HttpResponse

    :return: false if http status code is 2xx, raise an error otherwise
    :rtype bool

    :raise pymochow.exception.ClientError: if http status code is NOT 2xx
    """
    if http_response.status_code // 100 == http.client.OK // 100:
        return False
    if http_response.status_code // 100 == http.client.CONTINUE // 100:
        raise ClientError(b'Can not handle 1xx http status code')
    bse = None
    body = http_response.text
    if body:
        d = orjson.loads(compat.convert_to_string(body))
        bse = ServerError(d['msg'], code=d['code'])
    else:
        bse = ServerError(http_response.reason, request_id=response.metadata.bce_request_id)

    if bse is not None:
        bse.status_code = http_response.status_code
        raise bse
    else:
        raise ValueError("Error object is None")
