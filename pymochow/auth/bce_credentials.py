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
Provides access to the BCE credentials used for accessing BCE services: BCE access key ID and
secret access key.
These credentials are used to securely sign requests to BCE services.
"""
from pymochow import compat

class BceCredentials(object):
    """
    Provides access to VDB instance:
    account and api_key.
    """
    def __init__(self, account, api_key):
        """初始化实例对象
        
        Args:
            account (str): 访问用户。
            api_key (str): 访问密钥。
        
        Returns:
            None
        
        Raises:
            Exception: 无效的访问密钥或密钥密钥。
        """
        self.account = compat.convert_to_bytes(account)
        self.api_key = compat.convert_to_bytes(api_key)


class AppBuilderCredentials(object):
    """
    Provides access to VDB instance from appbuilder:
    account and api_key and appbuilder_token
    """
    def __init__(self, account, api_key, appbuilder_token):
        """初始化实例对象
        
        Args:
            account (str): 访问用户。
            api_key (str): 访问密钥。
            appbuilder_token (str): Appbuilder的token
        
        Returns:
            None
        
        Raises:
            Exception: 无效的访问密钥或密钥密钥。
        """
        self.account = compat.convert_to_bytes(account)
        self.api_key = compat.convert_to_bytes(api_key)
        self.appbuilder_token = compat.convert_to_bytes(appbuilder_token)
