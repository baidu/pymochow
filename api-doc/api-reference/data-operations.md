# 数据操作

## 概述

数据操作包括数据的插入、查询、更新和删除（CRUD）。

---

## 插入/更新数据

### upsert

使用 `upsert` 方法插入或更新数据。如果主键已存在则更新，否则插入。

```python
table.upsert(rows=rows)
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `rows` | `list[Row]` | 是 | 数据行列表 |

### Row 对象

使用 `Row` 创建数据行：

```python
from pymochow.model.table import Row

row = Row(
    id='doc_001',
    bookName='西游记',
    author='吴承恩',
    page=21,
    vector=[0.1, 0.2, 0.3, 0.4],
    arr_field=['tag1', 'tag2'],
    json_field={'key': 'value'}
)
```

### 完整示例

```python
from pymochow.model.table import Row

table = db.table('book_segments')

rows = [
    Row(
        id='0001',
        vector=[1, 0.21, 0.213, 0],
        bookName='西游记',
        author='吴承恩',
        page=21,
        arr_field=[],
        json_field={},
        segment='富贵功名，前缘分定，为人切莫欺心。'
    ),
    Row(
        id='0002',
        vector=[2, 0.22, 0.213, 0],
        bookName='西游记',
        author='吴承恩',
        page=22,
        arr_field=[],
        json_field={'page': 22},
        segment='正大光明，忠良善果弥深。'
    ),
]

table.upsert(rows=rows)
```

### 批量插入最佳实践

```python
# 建议每批 500-1000 条
BATCH_SIZE = 1000

def batch_insert(data):
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        table.upsert(rows=batch)
```

---

## 主键查询

### query

根据主键查询单条数据。

```python
result = table.query(
    primary_key=primary_key,
    projections=projections,
    retrieve_vector=retrieve_vector
)
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `primary_key` | `dict` | 是 | 主键字典 |
| `projections` | `list[str]` | 否 | 返回字段列表 |
| `retrieve_vector` | `bool` | 否 | 是否返回向量 |

**示例**:

```python
result = table.query(
    primary_key={'id': '0001'},
    projections=['id', 'bookName', 'author'],
    retrieve_vector=True
)
print(result)
```

---

## 批量查询

### batch_query

批量查询多条数据。

```python
from pymochow.model.table import BatchQueryKey

result = table.batch_query(
    keys=[
        BatchQueryKey({'id': '0001'}),
        BatchQueryKey({'id': '0002'}),
        BatchQueryKey({'id': '0003'}),
    ],
    projections=['id', 'bookName'],
    retrieve_vector=False
)
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `keys` | `list[BatchQueryKey]` | 是 | 主键列表 |
| `projections` | `list[str]` | 否 | 返回字段列表 |
| `retrieve_vector` | `bool` | 否 | 是否返回向量 |

---

## 条件筛选

### select

根据条件筛选数据，支持分页。

```python
result = table.select(
    filter=filter,
    projections=projections,
    marker=marker,
    limit=limit
)
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `filter` | `str` | 否 | 过滤条件 |
| `projections` | `list[str]` | 否 | 返回字段列表 |
| `marker` | `str` | 否 | 分页标记 |
| `limit` | `int` | 否 | 返回数量限制 |

**示例**:

```python
# 首次查询
result = table.select(
    projections=['id', 'bookName', 'json_field'],
    limit=20
)

# 分页查询
while result.is_truncated:
    result = table.select(
        projections=['id', 'bookName'],
        marker=result.next_marker,
        limit=20
    )
```

### 完整分页示例

```python
projections = ['id', 'bookName', 'json_field']
marker = None

while True:
    result = table.select(
        marker=marker,
        projections=projections,
        limit=20
    )
    
    for row in result.rows:
        print(row)
    
    if not result.is_truncated:
        break
    marker = result.next_marker
```

---

## 更新数据

### update

根据主键更新数据。

```python
table.update(
    primary_key=primary_key,
    update_fields=update_fields
)
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `primary_key` | `dict` | 是 | 主键字典 |
| `update_fields` | `dict` | 是 | 要更新的字段 |

**示例**:

```python
table.update(
    primary_key={'id': '0001'},
    update_fields={
        'bookName': '红楼梦',
        'author': '曹雪芹',
        'page': 21,
        'segment': '满纸荒唐言，一把辛酸泪'
    }
)
```

---

## 删除数据

### delete

根据主键删除数据。

```python
table.delete(primary_key=primary_key)
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `primary_key` | `dict` | 是 | 主键字典 |

**示例**:

```python
table.delete(primary_key={'id': '0001'})
```

### 批量删除最佳实践

> **警告**: 禁止大范围 delete 操作，建议通过 Select + 点删除拆分。

```python
def safe_delete(table, filter_condition, batch_size=1000):
    while True:
        result = table.select(
            filter=filter_condition,
            projections=['id'],
            limit=batch_size
        )
        
        if not result.rows:
            break
        
        for row in result.rows:
            table.delete(primary_key={'id': row.id})
```

---

## 表统计信息

### stats

获取表的统计信息。

```python
stats = table.stats()
print(f"行数: {stats}")
```

---

## 完整示例

```python
import time
from pymochow.model.table import Row, BatchQueryKey

# 获取表
table = db.table('book_segments')

# 1. 插入数据
rows = [
    Row(id='001', bookName='Book1', vector=[0.1, 0.2, 0.3, 0.4]),
    Row(id='002', bookName='Book2', vector=[0.2, 0.3, 0.4, 0.5]),
]
table.upsert(rows=rows)
time.sleep(1)

# 2. 主键查询
result = table.query(primary_key={'id': '001'}, projections=['id', 'bookName'])
print(f"查询结果: {result}")

# 3. 批量查询
result = table.batch_query(
    keys=[BatchQueryKey({'id': '001'}), BatchQueryKey({'id': '002'})],
    projections=['id', 'bookName']
)
print(f"批量查询: {result}")

# 4. 条件筛选
result = table.select(projections=['id', 'bookName'], limit=10)
print(f"筛选结果: {result.rows}")

# 5. 更新数据
table.update(
    primary_key={'id': '001'},
    update_fields={'bookName': 'Updated Book1'}
)

# 6. 删除数据
table.delete(primary_key={'id': '002'})

# 7. 统计信息
stats = table.stats()
print(f"统计: {stats}")
```

---

**相关文档**:
- [向量检索](./vector-search.md)
- [表操作](./table.md)
