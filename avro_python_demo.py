#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import avro.schema as schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

PY3 = sys.version_info[0] == 3
schema_parse = schema.Parse if PY3 else schema.parse

# 元数据，将json字符串转成schema对象，这个就是数据的描述信息
schema = schema_parse(open('user.avsc', 'rb').read())

# DatumWriter负责将python对象序列化成avro二进制格式
# DataFileWriter负责验证数据有效性并写入文件
# 文件的开头是的schema，之后是二进制的avro对象
writer = DataFileWriter(open('users.avro', 'wb'), DatumWriter(), schema)
writer.append(dict(name='Jenne', favorite_number=256))
writer.append(dict(name='Jerry', favorite_number=34, favorite_color='purple'))
writer.close()

reader = DataFileReader(open('users.avro', 'rb'), DatumReader())
for user in reader:
    print(user)
reader.close()

print('Enjoy Avro!')

