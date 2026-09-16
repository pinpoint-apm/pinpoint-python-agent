# pinpoint-python-agent
# Copyright (c) 2026-present NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Pinpoint service type codes.

Values come from the Pinpoint server's registered type dictionary. The two
Python-specific codes were assigned by product decision (2026-04-20):

- APP_TYPE_PYTHON = 1700            (Python application root)
- SERVICE_TYPE_PYTHON_METHOD = 1701 (Python function/method call)

The client/remote codes are the server's shared ones, so a Python service
renders in the call tree exactly like any other.
"""

APP_TYPE_PYTHON = 1700
SERVICE_TYPE_PYTHON_METHOD = 1701

# HTTP / web
SERVICE_TYPE_PYTHON_HTTP_CLIENT = 9900
SERVICE_TYPE_PYTHON_HTTP_SERVER = APP_TYPE_PYTHON  # root tx rides on APP_TYPE_PYTHON

# Databases
SERVICE_TYPE_UNKNOWN_DB = 2051
SERVICE_TYPE_MYSQL = 2101
SERVICE_TYPE_MSSQL = 2201
SERVICE_TYPE_ORACLE = 2301
SERVICE_TYPE_POSTGRESQL = 2501
SERVICE_TYPE_CASSANDRA = 2601
SERVICE_TYPE_MONGO = 2651

# Caches / queues
SERVICE_TYPE_MEMCACHED = 8050
SERVICE_TYPE_REDIS = 8203
SERVICE_TYPE_RABBITMQ_CLIENT = 8300
SERVICE_TYPE_KAFKA_CLIENT = 8660

# gRPC
SERVICE_TYPE_GRPC = 9160
SERVICE_TYPE_GRPC_SERVER = 1130

# Elasticsearch
SERVICE_TYPE_ELASTICSEARCH = 9204
