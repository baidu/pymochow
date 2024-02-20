# 百度向量数据库 Mochow Python SDK

针对百度智能云向量数据库，我们推出了一套 Python SDK（下称Mochow SDK），方便用户通过代码调用百度向量数据库。

## 如何安装

目前Mochow SDK 已发布到 PyPI ，用户可使用 pip 命令进行安装。安装Mochow SDK 需要 3.7.0 或更高的 Python 版本

```
pip install pymochow
```

在安装完成后，用户即可在代码内引入Mochow SDK 并使用

```python
import pymochow
```

## 快速使用

在使用Mochow SDK 之前，用户需要在百度智能云上创建向量数据库，以获得 API Key。API Key 是用户在调用Mochow SDK 时所需要的凭证。具体获取流程参见平台的[向量数据库使用说明文档](https://cloud.baidu.com/doc/VDB/index.html)。

获取到 API Key 后，用户还需要传递它们来初始化Mochow SDK。 可以通过如下方式初始化Mochow SDK：

```python
import pymochow
from pymochow.configuration import Configuration
from pymochow.auth.bce_credentials import BceCredentials

account = 'root'
api_key = 'your_api_key'
endpoint = 'you_endpoint' #example: http://127.0.0.1:8511

config = Configuration(credentials=BceCredentials(account, api_key),
            endpoint=endpoint)
client = pymochow.MochowClient(config)
```

## 功能

目前Mochow SDK 支持用户使用如下功能:

+ Databse 操作
+ Table 操作
+ Index 操作
+ Row 操作

## License

Apache-2.0

