# 安装指南

## 环境要求

- **Python 版本**: ≥ 3.7.0
- **操作系统**: Linux、macOS、Windows

## 安装方式

### pip 安装（推荐）

```bash
pip install pymochow
```

### 指定版本安装

```bash
pip install pymochow==x.x.x
```

### 从源码安装

```bash
git clone https://github.com/baidu/mochow-python-sdk.git
cd mochow-python-sdk
pip install .
```

## 验证安装

```python
import pymochow
print(pymochow.SDK_VERSION)  # 输出: b'2.3.4'
```

## 依赖说明

SDK 会自动安装以下依赖：

| 依赖 | 用途 |
|------|------|
| `requests` | HTTP 请求 |
| `urllib3` | HTTP 连接池 |

## 升级 SDK

```bash
pip install --upgrade pymochow
```

## 卸载 SDK

```bash
pip uninstall pymochow
```

## 常见问题

### Q: 安装时提示权限不足？

```bash
pip install --user pymochow
```

### Q: 网络问题导致安装失败？

使用国内镜像源：

```bash
pip install pymochow -i https://pypi.tuna.tsinghua.edu.cn/simple
```

---

**下一步**: [快速开始](./quickstart.md)
