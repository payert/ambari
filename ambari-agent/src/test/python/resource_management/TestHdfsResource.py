'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

from unittest import TestCase
from resource_management.libraries.providers.hdfs_resource import HdfsResourceWebHDFS
from resource_management.core.logger import Logger
from mock.mock import MagicMock

class TestHdfsResource(TestCase):

  def setUp(self):
    Logger.logger = MagicMock()

  def test_parse_hadoop_checksum(self):
    checksum = HdfsResourceWebHDFS._parse_hadoop_checksum(None, Logger)
    self.assertIsNone(checksum, "Checksum value should be None.")

    checksum = HdfsResourceWebHDFS._parse_hadoop_checksum("/tmp/install_kdc.sh	COMPOSITE-CRC32C	faa0ea32", Logger)
    self.assertEquals(checksum, "faa0ea32")

    checksum = HdfsResourceWebHDFS._parse_hadoop_checksum("/tmp/install_kdc.sh2	COMPOSITE-CRC32	d23d1869", Logger)
    self.assertIsNone(checksum, "Checksum value should be None because the COMPOSITE-CRC32 combine mode is not yet supported.")

    checksum = HdfsResourceWebHDFS._parse_hadoop_checksum("/tmp/test folder123/Neurotyping test Results.pdf	COMPOSITE-CRC32C	b882e764", Logger)
    self.assertEquals(checksum, "b882e764")

    checksum = HdfsResourceWebHDFS._parse_hadoop_checksum("/tmp/test folder123/Plant Paradox -1234.docx	COMPOSITE-CRC32C	9636e782", Logger)
    self.assertEquals(checksum, "9636e782")