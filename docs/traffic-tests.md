# Run a traffic flow test

Open **Hardware Offload → Run traffic test**, or open a traffic link in **Topology** and choose **Run traffic test**. Pick a test case and source node. For a cross-node case, also pick the destination node. A topology link preselects its nodes. You can change either endpoint before clicking **Start test**. Follow running and completed tests in **Traffic test history** on Hardware Offload. The Issues page also shows the three most recent runs. Select a run to open its saved measurements and per-node evidence.

The test pods run on the cluster default network in a unique
`ovn-light-tests-<run-id>` namespace. For a ClusterIP case, TFT also creates a
temporary Service. Policies selecting application pods can differ from those
selecting these test pods.

The picker runs TCP iperf with kernel hardware-offload validation, using
same-node or different-node placement and either pod IP or ClusterIP paths.
Each run measures the available TCP bitrate for 120 seconds and reports TX and
RX in Gbit/s. `TFT_PERF_MIN_TPUT` is fixed at `0`; there is no connectivity mode,
HTTP test or user-entered throughput threshold.

You can submit multiple runs. Requests are queued, and each runner starts once
TFT deletes the previous run’s user ConfigMap. Traffic runs can then overlap.
The iperf endpoints have separate pod IPs and use
neither host networking nor host ports. TFT's runner and tools use host
networking but do not bind the iperf server port.

## Use the TFT Helm chart

OVN-Light uses the traffic flow tests Helm chart.
Install that chart using its deployment instructions, values and registry
credentials. Keep its `ovn-sdn-flow-tests` CronJob suspended. Use a dedicated
chart installation for OVN-Light so its runner does not share configuration
with another test launcher. No TFT source or image changes are needed.

The flow-test namespace defaults to `ovn-k8s-flow-tests`. Set
`OVN_LIGHT_TRAFFIC_TEST_NAMESPACE` only when the chart is installed in another
namespace. For a server running directly on a host:

```sh
export OVN_LIGHT_TOKEN="$(cat .ovn-light-token)"
go run ./cmd/ovn-light --kubeconfig /etc/kubernetes/admin.conf \
  --enable-writes --data-dir .data
```

For the OVN-Light Deployment, apply `deploy/traffic-test-helm.yaml` and restart
the Deployment. That manifest grants access to the chart's CronJob, Jobs, logs
and configuration, and sets `OVN_LIGHT_TRAFFIC_TEST_NAMESPACE` through the
existing `ovn-light-traffic-test` ConfigMap. Change its namespace fields and
environment value if the chart is installed elsewhere. The chart owns the TFT
runner's service account, permissions, job image and endpoint image.

Each UI request is saved in the queue. When its turn arrives, OVN-Light creates
`ovn-sdn-ft-config-user` in the chart's namespace:

| UI input | User ConfigMap key |
| --- | --- |
| Test case | `TFT_TEST_CASES` |
| Source node | `TFT_REQ_REMOTE_CLIENT_NODE` |
| Destination node | `TFT_REQ_SERVER_NODE` |
| TCP throughput | `TFT_IPERF=true`, `TFT_CURL=false` |
| Test duration | `TFT_IPERF_TIME=120` |
| Minimum throughput | `TFT_PERF_MIN_TPUT=0` |
| Run namespace | `TFT_NAMESPACE=ovn-light-tests-<run-id>` |

TCP runs also set `TFT_PLUGINS=validate_offload`. TFT's existing plugin
finds each endpoint's VF representor and samples kernel ethtool RX and TX
packet counters before and after traffic. OVN-Light shows its native client/server
verdicts and RX/TX packet-counter increases alongside throughput. The TFT runner
log contains the sampled counters and failure details; TFT owns the validation
thresholds and logic.

OVN-Light then creates a Job from the installed CronJob's `jobTemplate`, keeping
its image, entrypoint, service account, pull secrets and resource configuration.
The Job imports `ovn-sdn-ft-config` first and `ovn-sdn-ft-config-user` second.
TFT reads the environment values, deletes the user ConfigMap, generates its
configuration and runs `tft.py --check`. OVN-Light recreates the user ConfigMap
for the next queued request after the previous runner deletes it, without waiting
for that test to finish. It never overwrites an existing ConfigMap. The
chart's `TFT_SKIP_TESTS` is preserved, and selecting a skipped case is rejected.

Runs disable additional target-access subtests and deferred cleanup. TFT creates
and cleans up its own endpoints. OVN-Light records the native runner's summary
and log before deleting its Job. Running tests requires the server's
write-enabled mode. Images and endpoint resources come from the chart; OVN-Light
accepts only the flow-test namespace as runner configuration.

## How to read the result

Collectors use their usual collection interval. While temporary test pods exist,
their nodes include a bounded set of cached OVS rules anchored to each pod's
local datapath port and UID. Open the software, partial-offload, hardware or
unknown groups under each node to inspect matches, actions and counters. A
shared rule's counters can include other traffic. OVN-Light retains up to 32 flow
entries per node per run and limits raw lines to 4 KiB. It matches pod endpoint
tuples and the configured test server port; client ports are not fixed. A
Service path can expose the virtual IP before translation. Short tests can finish
between samples, and missing or masked evidence can leave a node inconclusive.
Software-only KIND nodes cannot demonstrate hardware offload.

Expand a matched path for individual recirculation IDs, stage states, provider reasons and partial-packet counter deltas. DOCA entries show the zone, observed direction, original and reply offload flags, and any deferred-reply reason supplied by the provider. Deltas may cover shared stages. A persistent issue requires the collector’s usual activity and grace checks. A successful transfer can still contain software stages. If the collector identifies a matching denying ACL, its details accompany the traffic result. [Hardware offload evidence](offload.md)

Each run uses a unique `ovn-light-tft-<run-id>` Job name. Jobs disable automatic
retries and container restarts so an older runner cannot consume a newer run’s
configuration. OVN-Light persists queued and active runs along with the latest
30 completed runs, recovers its Jobs after a server restart, and deletes each
completed Job after collecting its result. TFT cleans up its endpoint
namespace. The chart's Job TTL also covers interrupted Job cleanup. Evidence is
collected while the server is running; a server interruption can leave gaps.
