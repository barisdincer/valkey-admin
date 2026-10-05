# Troubleshooting

- [Troubleshooting](https://valkey-admin.valkey.io/reference/troubleshooting/) — common issues and fixes.
- [Known Limitations](https://valkey-admin.valkey.io/reference/limitations/) — capabilities and operational caveats.

Direct cluster-node hostnames may resolve to IPs advertised by `CLUSTER SLOTS`.
The connected seed probe's CLIENT INFO endpoint is matched against advertised
primaries and replicas at the same port, sharing that node's metrics collector.
DNS must work inside the Admin container. If the connected endpoint cannot be
verified (for example, CLIENT INFO is denied by ACL), the seed keeps its own collector.
