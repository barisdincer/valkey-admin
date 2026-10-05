# Troubleshooting

- [Troubleshooting](https://valkey-admin.valkey.io/reference/troubleshooting/) — common issues and fixes.
- [Known Limitations](https://valkey-admin.valkey.io/reference/limitations/) — capabilities and operational caveats.

Direct cluster-node hostnames may resolve to IPs advertised by `CLUSTER SLOTS`.
When exactly one advertised node matches the resolved address and port, its metrics
collector is shared with the hostname connection. DNS must work inside the Admin
container; multiple matching nodes keep the seed's own collector.
