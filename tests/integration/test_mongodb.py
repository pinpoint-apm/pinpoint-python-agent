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

"""pymongo against a real MongoDB container.

pymongo instrumentation registers a global CommandListener, which pymongo
snapshots at MongoClient construction — so ``instrument()`` runs before the
client fixture builds the client.
"""

from __future__ import annotations

import pytest

pymongo = pytest.importorskip("pymongo")

from pinpoint.annotation import (
    ANNOTATION_MONGO_COLLECTION_INFO,
    ANNOTATION_MONGO_JSON_DATA,
)
from pinpoint.service_type import SERVICE_TYPE_MONGO

_DB = "it_db"
_COLLECTION = "it_events"


@pytest.fixture(scope="module", autouse=True)
def _instrument():
    from pinpoint.instrumentations import pymongo as instr

    instr.instrument()


@pytest.fixture(scope="module")
def client(mongo_container, _instrument):
    c = pymongo.MongoClient(mongo_container["url"])
    # Force connection setup (auth handshake etc.) outside any traced block so
    # the tests below only see their own commands.
    c.admin.command("ping")
    yield c
    c.close()


def test_insert_and_find_record_events(client, mongo_container, traced,
                                       sql_bind_values_on):
    coll = client[_DB][_COLLECTION]
    coll.insert_one({"name": "alice", "n": 1})
    docs = list(coll.find({"name": "alice"}))
    assert len(docs) == 1

    insert_ev = traced.single("mongo.insert")
    assert insert_ev.ended
    assert insert_ev.service_type == SERVICE_TYPE_MONGO
    assert insert_ev.destination == _DB
    assert insert_ev.endpoint.endswith(str(mongo_container["port"]))
    assert insert_ev.ann(ANNOTATION_MONGO_COLLECTION_INFO) == [_COLLECTION]
    # The marshaled wire command rides in the two-string JSON annotation.
    json_anns = [a for a in insert_ev.annotations
                 if a[1] == ANNOTATION_MONGO_JSON_DATA]
    assert json_anns and "insert" in json_anns[0][2]

    find_ev = traced.single("mongo.find")
    assert find_ev.ended
    assert find_ev.ann(ANNOTATION_MONGO_COLLECTION_INFO) == [_COLLECTION]


def test_command_payload_not_captured_by_default(client, traced):
    """The secure default: without ``sql_trace_bind_values`` the command name
    and collection are recorded, but not the marshaled document payload."""
    client[_DB][_COLLECTION].insert_one({"secret": "sensitive-value"})

    ev = traced.single("mongo.insert")
    assert ev.ended
    assert ev.ann(ANNOTATION_MONGO_COLLECTION_INFO) == [_COLLECTION]
    assert [a for a in ev.annotations
            if a[1] == ANNOTATION_MONGO_JSON_DATA] == []


def test_failed_command_records_error(client, traced):
    with pytest.raises(pymongo.errors.OperationFailure):
        client[_DB].command({"noSuchCommandIt": 1})

    failed = [e for e in traced.events if e.error is not None]
    assert failed and failed[-1].ended
    assert failed[-1].operation.startswith("mongo.")


def test_no_current_span_records_nothing(client, recorder):
    client[_DB][_COLLECTION].count_documents({})
    assert recorder.events == []
