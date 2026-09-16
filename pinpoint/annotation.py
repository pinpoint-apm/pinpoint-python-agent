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

"""Annotation key codes, as registered in the Pinpoint server's dictionary."""

ANNOTATION_ARG0 = -1
ANNOTATION_API = 12
ANNOTATION_SQL_ID = 20
ANNOTATION_HTTP_URL = 40
ANNOTATION_HTTP_PARAM = 41
ANNOTATION_HTTP_COOKIE = 45
ANNOTATION_HTTP_STATUS_CODE = 46
ANNOTATION_HTTP_REQUEST_HEADER = 47
ANNOTATION_HTTP_RESPONSE_HEADER = 55
ANNOTATION_HTTP_PROXY_HEADER = 300
ANNOTATION_KAFKA_TOPIC = 140
ANNOTATION_KAFKA_PARTITION = 141
ANNOTATION_KAFKA_OFFSET = 142
ANNOTATION_KAFKA_BATCH = 143
ANNOTATION_KAFKA_HEADER = 144
ANNOTATION_MONGO_JSON_DATA = 150
ANNOTATION_MONGO_COLLECTION_INFO = 151
ANNOTATION_MONGO_COLLECTION_OPTION = 152
ANNOTATION_GRPC_CLIENT_STATUS = 160
ANNOTATION_ELASTICSEARCH_DSL = 173
ANNOTATION_ELASTICSEARCH_VERSION = 176
ANNOTATION_RABBITMQ_EXCHANGE = 130
ANNOTATION_RABBITMQ_ROUTINGKEY = 131
