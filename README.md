## Waku Storage Retriever

This Python script facilitates arbitrary GET and POST requests
to pods in a Kubernetes cluster.
The script is designed to run inside a Docker container.

### Usage
The script can be run in one of two modes: `server` or `batch`.

#### Batch Mode

`python ./api_requester.py --mode batch --config /mount/config.yaml`

Runs all actions in the config sequentially, then exits. 

Add `--namespace <ns>` when any target is resolved through the Kubernetes API.
It can be omitted when every target is static, which is how the requester runs 
in environments without a Kubernetes control plane such as the Shadow network simulator.

See `examples/shadow_publish.yaml` for a complete batch config.

#### Server Mode

`python ./api_requester.py --mode server --config /mount/config.yaml`

Runs a server, allowing scripts to call API endpoints, causing this pod to make API requests to other pods.

See endpoints under `def create_app` in `api_requester.py` for usage details.

### Config Format

The ConfigMap in `config.yaml` defines the config objects.
Class definitions are in `configs.py`.

The idea is that a user can define various pieces of the config,
then combine them as needed. Each object stands independently, but
they work together when running an action or making a request.

Each config object has a name by which it can be referenced.
Some fields are optional.

Endpoints - Defines an API endpoint for a request. May set a `timeout` (seconds, default 30).
Targets - Defines a set of filters to use to determine if pods on a cluster are part of the target. May instead be resolved by hostname — see "Static targets".
Requests - Contains an Endpoint and some additional information for retries and delays.
Actions - Combines Targets and Requests into a defined action, representing a series of requests. May set a `delay` (seconds) to pace requests.

#### Static targets (no Kubernetes)

By default a Target is resolved through the Kubernetes API (by `service`,
`name_template`, and/or `stateful_set`). A Target can instead be resolved purely
by hostname via `hosts` — an explicit list, e.g. `["pod-0", "pod-1"]` — with no
Kubernetes API calls, useful for the Shadow simulator or any environment without
a control plane.

The hostname is substituted into the endpoint URL's `{node}` and resolved by the
HTTP client's OS resolver at request time. Static and Kubernetes targets can be
mixed in the same config.

#### How an Action is performed

1. For each ConfigTarget, add all pods to the list "all_pods". Note: No deduplication is done.
2. Sort the list of pods according to `order`.
3. Starting at `pod_start_index`, take `pod_count` pods. Note: Loop through the list if needed to get `pod_count` items.
4. According to `loop_order`, make every request in `requests` to every pod in the remaining list.

See docstrings in the `ConfigAction` class for more details.

### Logos Storage operations

Use the production image in two roles: one central requester coordinates operations,
and a verifier sidecar in each ordinary Storage pod consumes file streams over
localhost. The Logos Storage experiment selects the new image tag
`albertodst/pod-api-requester:logos-storage-v3` for both roles.

The central server exposes `POST /storage/upload` and `POST /storage/download`.
Each request selects exactly one pod using an existing named or inline target.
Use an anchored `name_template`, such as `^storage-nodes-0$`, and set the target's
`port` to the Storage API port (normally 8080).
No matching pod returns 404; multiple matches return 409.

Upload a random file with:

```json
{
  "target": {
    "kind": "config",
    "value": {"name": "uploader", "name_template": "^storage-nodes-0$", "port": 8080}
  },
  "file_size_bytes": 1048576,
  "timeout_seconds": 600
}
```

The coordinator generates 64 KiB chunks, computes SHA-256, and streams them to
`POST /api/storage/v1/data` on the publisher. The existing requester envelope
contains `response.text`, a JSON receipt with `cid`, `sha256`,
`file_size_bytes`, and `seconds`. The payload is not saved to a file or returned
to the experiment runner. The default file size is 1 MiB.

Pass the upload's CID, hash, and size to a different receiver:

```json
{
  "target": {
    "kind": "config",
    "value": {"name": "receiver", "name_template": "^storage-nodes-1$", "port": 8080}
  },
  "cid": "<returned CID>",
  "expected_sha256": "<64 lowercase hex characters from upload receipt>",
  "expected_size_bytes": 1048576,
  "verifier_port": 8646,
  "transport": "mix",
  "timeout_seconds": 600
}
```

The coordinator sends only this operation's JSON fields to
`http://<receiver-pod-ip>:8646/download`. It never consumes the file stream.
`verifier_port` defaults to 8646. A missing sidecar is an error, with no fallback
to central streaming or to Storage's background/local-read endpoints.

Start the verifier with the same image, overriding the production entrypoint:

```sh
python -m routers.storage_verifier --port 8646 --storage-port 8080
```

This mode needs no config file, Kubernetes API access, or namespace discovery.
It exposes `GET /healthz` and `POST /download`. The download body contains
`cid`, `expected_sha256`, `expected_size_bytes`, `transport`, and
`timeout_seconds` only. The upstream host is fixed to 127.0.0.1.
The sidecar calls
`GET http://127.0.0.1:8080/api/storage/v1/data/<cid>/network/stream?transport=mix`
(or `transport=direct`) and hashes the body as Storage downloads and stores it.

The verifier consumes 64 KiB chunks and discards them after hashing. It neither
writes a payload file nor buffers the complete body. A successful response
requires normal stream completion, the expected actual byte count, and a
matching SHA-256. A truncated declared Content-Length is also a stream error.
HTTP redirects are disabled, content decompression is disabled, and error bodies
are bounded. Each retry starts a fresh checksum; Storage can reuse cached blocks.

The sidecar returns a small receipt, which the coordinator validates and places
inside the usual `response.text` envelope:

```json
{
  "cid": "<file CID>",
  "sha256": "<actual SHA-256>",
  "file_size_bytes": 1048576,
  "seconds": 1.2,
  "transport": "mix",
  "verified": true,
  "verification_location": "receiver_pod"
}
```

File bodies cross the Mix/direct network into Storage and then cross localhost
to the verifier. Only operation metadata and receipts cross the connection
between the coordinator and a receiver during downloads. No second local read,
progress polling, or background-download POST is used.

The coordinator allows eight active Storage operations per worker. Each verifier
allows one active stream by default; `--max-concurrent-downloads` can change that.
`timeout_seconds` defaults to 600, covering capacity wait, pod discovery, verifier
queueing, and streaming. The coordinator forwards the remaining budget after
discovery. Timing in the download receipt measures the stream and hashing pass;
the experiment's elapsed time includes coordination and retries.

Checksum/size mismatches return 409 from the verifier. The coordinator wraps
verifier HTTP errors in 502 with `detail.upstream_status_code` identifying the
cause. Original Storage HTTP statuses are preserved through the verifier hop
so callers can distinguish permanent failures from transient errors.
Other errors include 422 for invalid inputs, 503 for
unavailable pod information, and 504 for the coordinator's own deadline.
The experiment owns retry policy. The verifier closes its HTTP stream on timeout
or failure; neither component issues background-download cancellation requests.

### Development checks

Install `requirements-dev.txt` in a development environment, then run:

```sh
python -m unittest discover -s tests -v
```

Storage tests use a local fake Storage HTTP server and mocked Kubernetes pod
discovery; they require no cluster. The existing Dockerfile already copies the
new router. Build a new requester image with `build.sh` before using these
endpoints in an experiment.

### Files

```
api_requester.py    Main code that will run on the pod-api-requester pod
bind.yaml           Necessary Kubernetes Role + RoleBinding to give permission to list pods
config.yaml         Kubernetes config containing the definitions for Targets, Endpoints, Requests, and Actions

build.sh            Sample commands to build the Docker container
Dockerfile          File to build the pod-api-requester image

deployment.yaml     Sample pod for development/testing
client.py           Sample code to make API requests directed at a pod running this code
```


### Changelog

- `v3.1.0`:
  - Added static / DNS-based target resolution (`hosts`: an explicit hostname list),
    so targets can be addressed by hostname with no Kubernetes API calls (e.g. inside the
    Shadow simulator).
  - `--mode batch` no longer requires a Kubernetes connection when all targets are static:
    the in-cluster client is created lazily, and `--namespace` is optional in that case.
  - Added `delay` on Actions (pacing between requests) and `timeout` on Endpoints
    (per-request HTTP timeout, default 30s — previously requests could block indefinitely).
  - Fixed the batch-mode action path (target/action iteration, namespace passing) and made
    pod ordering numeric (`pod-2` before `pod-10`).
- `v3.0.0`:
  - Added **load test mode** for high-throughput async testing
  - New `LoadTestConfig` in actions with rate limiting and burst support
  - Added `async_client.py` for aiohttp-based requests
  - Added `/loadtest/*` API endpoints for server mode
- `v2.0.0`:
  - Changed to using a ConfigMap to define Targets, Endpoints, Requests, and Actions
  - Added server capability
  - Removed --debug mode logic
- `v1.0.1`:
  - Added `--debug` mode. Makes multiple API requests to each IP
  - Added `--select-types` mode
    - Use the node type flags to determine which nodes to use for API requests
    - All API requests will be made to all nodes specified
    - Accepted arguments are a number or `all` (eg. --store=1 --relay=all)
    - Without using this flag, the script will use the old behavior
      (randomly choose a node using zerotesting-service)
- `v1.0.0`:
  Initial version

---

### Load Test Mode

Actions can run in **load test mode** for high-throughput async testing.

#### Enabling Load Test Mode

Add a `load_test` section to any action in `config.yaml`:

```yaml
actions:
  - name: gossip-burst
    requests: ["gossip-publish"]
    targets: ["nim-nodes"]
    order: ascending
    loop_order: foreach_pod_make_all_requests
    load_test:
      enabled: true
      rate_per_pod: 128.0       # messages/second per pod
      messages_per_pod: 512     # total messages per pod
```

#### Load Test Configuration Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable load test mode |
| `rate_per_pod` | float | - | Messages per second per pod |
| `messages_per_pod` | int | - | Total messages per pod |
| `duration_seconds` | float | - | Run duration (alternative to messages_per_pod) |
| `burst_size` | int | - | Messages per burst |
| `burst_delay` | float | - | Delay between bursts |
| `parallel_workers` | bool | `true` | Run workers in parallel |
| `request_timeout` | float | `30.0` | HTTP request timeout |

#### HTTP API Endpoints (Server Mode)

```bash
# List actions with load_test enabled
GET /loadtest/actions

# Run a pre-configured load test action
POST /loadtest/run/{action_name}

# Run an inline load test
POST /loadtest/inline
```