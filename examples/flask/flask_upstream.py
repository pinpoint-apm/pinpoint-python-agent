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

"""Upstream Flask service paired with flask_demo.py.

Run alongside flask_demo.py to exercise distributed tracing end to end:

    pinpoint-run --app-name demo-upstream --agent-name demo-upstream \
        --collector localhost -- python examples/flask/flask_upstream.py

Or manually:
    import pinpoint
    pinpoint.init(
        application_name="demo-upstream",
        agent_name="demo-upstream",
        server_info="Flask Upstream",
    )
    pinpoint.autoload.autoload()

flask_demo.py issues an outbound `requests.get()` to /echo here. The
agent's requests-instrumentation injects Pinpoint-* headers, the agent's
flask-instrumentation extracts them on entry, so both spans land under
the same TraceID on the collector.
"""

import flask

app = flask.Flask(__name__)


@app.route("/echo")
def echo():
    return {"service": "demo-upstream", "ok": True}, 200


if __name__ == "__main__":
    # Manual init path (remove this block if launching via pinpoint-run).
    import pinpoint
    from pinpoint.autoload import autoload

    pinpoint.init(
        application_name="python-demo-upstream",
        agent_name="python-demo-upstream-1",
        server_info="Flask Upstream",
        # Header tracing — see flask_demo.py for the equivalent env vars.
        http_server_record_request_header=[
            "User-Agent", "Content-Type", "Accept", "Host",
            "X-Request-ID", "X-Forwarded-For",
        ],
        # Match flask_demo.py's outbound cookie so the upstream root span
        # records the same ``session_id`` value the caller sent.
        http_server_record_request_cookie=["session_id", "token"],
        http_server_record_response_header=["Content-Type"],
    )
    autoload()

    app.run(host="0.0.0.0", port=5001)
