import { afterEach, beforeEach, describe, it, mock } from "node:test"
import assert from "node:assert/strict"
import { GlideClient, GlideClusterClient, InfoOptions } from "@valkey/valkey-glide"
import { buildConnectionId, toNodeId, VALKEY, COMMANDLOG_TYPE, METRICS_SERVER_NOT_READY } from "valkey-common"
import { connectToValkey, closeMetricsServer, getMetricsNodeId } from "../connection"
import { clients, clusterNodesRegistry, clusterCredentials, metricsServerMap, startMetricsServer, __test__ } from "../metrics-orchestrator"
import { dns } from "../utils"
import { ensureSession, authorizeConnection, _resetSessions } from "../session"
import { subscribe, _reset as resetWatchers } from "../node-watchers"
import { setData } from "../actions/stats"
import { cpuUsageRequested } from "../actions/cpuUsage"
import { memoryUsageRequested } from "../actions/memoryUsage"
import { monitorRequested } from "../actions/monitorAction"
import { hotKeysRequested } from "../actions/hotkeys"
import { bigKeysRequested } from "../actions/bigkeys"
import { commandLogsRequested } from "../actions/commandLogs"
import { runConfigPushSession } from "../actions/config"
import type WebSocket from "ws"
import type { IncomingMessage } from "node:http"
import type { ChildProcess, SpawnOptions } from "node:child_process"
import type { Deps, ReduxAction } from "../actions/utils"

const HOST = "node.example.com"
const IP = "192.0.2.10"
const NODE_ID = toNodeId(buildConnectionId(IP, 6379))
const SLOTS = [
  [0, 8191, [IP, 6379, "primary-1"]],
  [8192, 16383, ["192.0.2.11", 6379, "primary-2"]],
]

describe("cluster connection metrics identity", () => {
  let sessionId: string
  let spawned: SpawnOptions["env"][]
  let messages: string[]
  let ws: WebSocket
  let connectedNodesByCluster: Map<string, string[]>
  let standaloneCreates: number
  let clusterHosts: string[]
  let slots: typeof SLOTS
  let probeInfo: unknown
  let probeInfoError: Error | undefined

  beforeEach(() => {
    clients.clear()
    metricsServerMap.clear()
    clusterNodesRegistry.clear()
    clusterCredentials.clear()
    _resetSessions()
    resetWatchers()
    spawned = []
    messages = []
    connectedNodesByCluster = new Map()
    standaloneCreates = 0
    clusterHosts = []
    slots = SLOTS
    probeInfo = "id=1 addr=192.0.2.100:50000 laddr=" + IP + ":6379 name=probe\r\n"
    probeInfoError = undefined
    sessionId = ensureSession({ headers: {}, socket: {} } as IncomingMessage).sessionId
    ws = { OPEN: 1, readyState: 1, send: (message: string) => messages.push(message) } as unknown as WebSocket
    mock.method(dns, "lookup", async () => [{ address: IP, family: 4 }])
    mock.method(dns, "reverse", async () => [])
    mock.method(__test__, "spawnProcess", (_command: string, _args: string[], options: SpawnOptions) => {
      spawned.push(options.env)
      return { pid: 1000 + spawned.length, stderr: null, on: () => {} } as unknown as ChildProcess
    })
    const standalone = {
      info: async (sections: string[]) => sections.includes(InfoOptions.Server)
        ? "valkey_version:9.0.0\r\n" : "cluster_enabled:1\r\n",
      customCommand: async (args: string[]) => {
        if (args[0] === "CLUSTER") return slots
        if (args[0] === "CLIENT" && args[1] === "INFO") {
          if (probeInfoError) throw probeInfoError
          return probeInfo
        }
        return []
      },
      configGet: async () => ({ "cluster-databases": "16" }),
      close: () => {},
    }
    // Keep instanceof meaningful so the existing-client reuse path is exercised.
    const cluster = Object.assign(Object.create(GlideClusterClient.prototype), {
      info: async () => ({ [IP]: "maxmemory_policy:noeviction\r\n" }),
      customCommand: async (args: string[]) => args[1] === "SLOTS" ? slots : [],
      configGet: async () => ({ "cluster-databases": "16" }),
      close: () => {},
    })
    mock.method(GlideClient, "createClient", async () => {
      standaloneCreates += 1
      return standalone
    })
    mock.method(GlideClusterClient, "createClient", async (options: Parameters<typeof GlideClusterClient.createClient>[0]) => {
      clusterHosts.push(options.addresses[0].host)
      return cluster
    })
  })

  afterEach(() => {
    mock.restoreAll()
    clients.clear()
    metricsServerMap.clear()
    clusterNodesRegistry.clear()
    clusterCredentials.clear()
    __test__.collectorKeys.clear()
    resetWatchers()
    _resetSessions()
  })

  const context = () => ({ clients, metricsServerMap, clusterNodesRegistry, connectedNodesByCluster })
  const deps = (connectionId: string): Deps => ({ ...context(), ws, connectionId, sessionId })
  const action = (connectionId: string, payload: Record<string, unknown> = {}): ReduxAction => ({
    type: "test", payload: { connectionId, ...payload }, meta: undefined,
  })

  async function connect(host = HOST, port = "6379", db = 0) {
    const connectionId = buildConnectionId(host, port, db)
    const result = await connectToValkey(context(), ws, {
      connectionId, sessionId,
      connectionDetails: { host, port, db, endpointType: "node", tls: false, verifyTlsCertificate: true },
    })
    assert.ok(result)
    authorizeConnection(sessionId, connectionId)
    return connectionId
  }

  it("keeps an IP connection on the advertised collector", async () => {
    const id = await connect(IP)
    assert.equal(getMetricsNodeId(id, clients), NODE_ID)
    assert.equal(spawned.filter((env) => env?.VALKEY_HOST === IP).length, 1)
  })

  it("maps a hostname and its db-less UI alias to the advertised IP without changing the seed", async () => {
    const id = await connect()
    assert.equal(getMetricsNodeId(id, clients), NODE_ID)
    assert.equal(getMetricsNodeId(toNodeId(id), clients), NODE_ID)
    assert.equal(spawned.filter((env) => env?.VALKEY_HOST === IP).length, 1)
    assert.equal(spawned.some((env) => env?.VALKEY_HOST === HOST), false)
    const reply = messages.map((message) => JSON.parse(message))
      .find((message) => message.type === VALKEY.CONNECTION.clusterConnectFulfilled)
    assert.equal(reply.payload.connectionId, id)
    assert.equal(reply.payload.address.host, HOST)
    assert.deepEqual(clusterHosts, [HOST])
  })

  it("reuses an IP collector for an authorized hostname connection instead of spawning another", async () => {
    await connect(IP)
    const id = await connect()
    assert.equal(getMetricsNodeId(id, clients), NODE_ID)
    assert.equal(standaloneCreates, 1)
    assert.equal(spawned.filter((env) => env?.VALKEY_HOST === IP).length, 1)
    assert.equal(metricsServerMap.has(toNodeId(id)), false)
  })

  it("does not duplicate collectors for concurrent hostname/IP/database connections", async () => {
    const ids = await Promise.all([connect(), connect(IP), connect(HOST, "6379", 1)])
    for (const id of ids) assert.equal(getMetricsNodeId(id, clients), NODE_ID)
    assert.equal(spawned.filter((env) => env?.VALKEY_HOST === IP).length, 1)
    assert.equal(spawned.some((env) => env?.VALKEY_HOST === HOST), false)
  })

  it("deduplicates concurrent and repeated starts at the collector boundary", async () => {
    const node = { host: IP, port: 6379, tls: false, verifyTlsCertificate: true }
    await Promise.all([startMetricsServer(node, NODE_ID), startMetricsServer(node, NODE_ID)])
    assert.equal(spawned.length, 1)
    await startMetricsServer(node, NODE_ID)
    assert.equal(spawned.length, 1)
  })

  it("falls back to the seed when DNS fails rather than selecting an unrelated cluster node", async () => {
    probeInfo = "id=1"
    mock.method(dns, "lookup", async () => { throw new Error("ENOTFOUND") })
    mock.method(console, "warn", () => {})
    const id = await connect()
    assert.equal(getMetricsNodeId(id, clients), toNodeId(id))
    assert.equal(spawned.filter((env) => env?.VALKEY_HOST === HOST).length, 1)
  })

  it("does not match the same IP on a different port", async () => {
    const id = await connect(HOST, "6380")
    assert.equal(getMetricsNodeId(id, clients), toNodeId(id))
  })

  it("does not choose arbitrarily when DNS matches multiple advertised nodes", async () => {
    probeInfo = "id=1"
    mock.method(dns, "lookup", async () => [
      { address: IP, family: 4 }, { address: "192.0.2.11", family: 4 },
    ])
    const id = await connect()
    assert.equal(getMetricsNodeId(id, clients), toNodeId(id))
  })

  it("does not evict a collector still claimed by a hostname alias during reconciliation", async () => {
    await connect()
    const diff = await __test__.findDiff(metricsServerMap, {})
    assert.equal(diff.nodesToRemove.includes(NODE_ID), false)
  })

  it("keeps a shared collector alive and closes it with the canonical ID for the last alias", async () => {
    const hostnameId = await connect()
    const ipId = await connect(IP)
    metricsServerMap.get(NODE_ID)!.metricsURI = "http://127.0.0.1:9001"
    const fetch = mock.method(globalThis, "fetch", async () => new Response("", { status: 200 }))
    await closeMetricsServer(hostnameId, metricsServerMap, clients)
    assert.equal(fetch.mock.calls.length, 0)
    clients.delete(ipId)
    await closeMetricsServer(hostnameId, metricsServerMap, clients)
    assert.equal(fetch.mock.calls.length, 1)
    assert.deepEqual(JSON.parse(fetch.mock.calls[0].arguments[1]!.body as string), { connectionId: NODE_ID })
    assert.equal(metricsServerMap.has(NODE_ID), false)
  })

  for (const ipFirst of [false, true]) {
    it("shares the advertised replica collector with its hostname, IP first: " + ipFirst, async () => {
      const replicaIp = "192.0.2.20"
      const replicaId = toNodeId(buildConnectionId(replicaIp, 6379))
      slots = [[0, 16383, [IP, 6379, "primary-1"], [replicaIp, 6379, "replica-1"]]]
      probeInfo = "id=1 laddr=" + replicaIp + ":6379"
      mock.method(dns, "lookup", async () => [{ address: replicaIp, family: 4 }])
      if (ipFirst) await connect(replicaIp)
      const hostnameId = await connect("replica.example.com")
      if (!ipFirst) await connect(replicaIp)
      assert.equal(getMetricsNodeId(hostnameId, clients), replicaId)
      assert.equal(getMetricsNodeId(toNodeId(hostnameId), clients), replicaId)
      assert.equal(spawned.filter((env) => env?.VALKEY_HOST === replicaIp).length, 1)
      assert.equal(spawned.some((env) => env?.VALKEY_HOST === "replica.example.com"), false)
    })
  }

  for (const reuse of [false, true]) {
    it("does not change metrics identity when a later DNS answer rotates, cached client: " + reuse, async () => {
      if (reuse) await connect(IP)
      let lookups = 0
      mock.method(dns, "lookup", async () => [{
        address: ++lookups === 1 ? IP : "192.0.2.11", family: 4,
      }])
      const id = await connect()
      assert.equal(getMetricsNodeId(id, clients), NODE_ID)
      assert.equal(lookups, 1, "collector selection must not make another DNS lookup")
      assert.equal(spawned.some((env) => env?.VALKEY_HOST === HOST), false)
    })
  }

  it("matches a bracketed IPv6 connected endpoint to its advertised node", async () => {
    const ip = "2001:db8::10"
    slots = [[0, 16383, [ip, 6379, "primary-1"]]]
    probeInfo = "id=1 laddr=[" + ip + "]:6379"
    const id = await connect()
    assert.equal(getMetricsNodeId(id, clients), toNodeId(buildConnectionId(ip, 6379)))
    assert.equal(spawned.filter((env) => env?.VALKEY_HOST === ip).length, 1)
  })

  const unavailablePeerInfo = [
    { name: "ACL denies CLIENT INFO", response: undefined, error: new Error("NOPERM") },
    { name: "laddr is absent", response: "id=1 addr=192.0.2.100:50000" },
    { name: "response is not a string", response: [] },
    { name: "address is not advertised", response: "id=1 laddr=192.0.2.99:6379" },
    { name: "connected port does not match", response: "id=1 laddr=" + IP + ":6380" },
  ]
  for (const peerInfo of unavailablePeerInfo) {
    it("retains the seed identity when " + peerInfo.name, async () => {
      probeInfo = peerInfo.response
      probeInfoError = peerInfo.error
      const id = await connect()
      assert.equal(getMetricsNodeId(id, clients), toNodeId(id))
      assert.equal(spawned.filter((env) => env?.VALKEY_HOST === HOST).length, 1)
    })
  }

  it("uses the connected address even if DNS returns multiple candidate nodes", async () => {
    mock.method(dns, "lookup", async () => [
      { address: IP, family: 4 }, { address: "192.0.2.11", family: 4 },
    ])
    const id = await connect()
    assert.equal(getMetricsNodeId(id, clients), NODE_ID)
    assert.equal(spawned.some((env) => env?.VALKEY_HOST === HOST), false)
  })

  for (const monitorCase of [
    { name: "start", monitorAction: "start", fails: false },
    { name: "stop", monitorAction: "stop", fails: false },
    { name: "stop when the collector is unreachable", monitorAction: "stop", fails: true },
    { name: "status", monitorAction: "status", fails: false },
  ]) {
    it("broadcasts cluster Monitor " + monitorCase.name + " to alias watchers once per socket", async () => {
      const hostnameId = await connect()
      const ipId = await connect(IP)
      const clusterId = clients.get(hostnameId)!.clusterId!
      metricsServerMap.get(NODE_ID)!.metricsURI = "http://127.0.0.1:9001"
      const response = { monitorRunning: monitorCase.monitorAction === "start", checkAt: null, startedAt: null }
      mock.method(globalThis, "fetch", async () => {
        if (monitorCase.fails) throw new Error("collector unavailable")
        return Response.json(response)
      })
      const aliasMessages: string[] = []
      const sharedMessages: string[] = []
      const unrelatedMessages: string[] = []
      const watcher = (out: string[]) => ({ send: (message: string) => out.push(message) }) as unknown as WebSocket
      const aliasWs = watcher(aliasMessages)
      const sharedWs = watcher(sharedMessages)
      subscribe(hostnameId, aliasWs)
      for (const id of [hostnameId, ipId, NODE_ID]) {
        subscribe(id, sharedWs)
        subscribe(id, ws)
      }
      subscribe(toNodeId(buildConnectionId("192.0.2.11", 6379)), watcher(unrelatedMessages))
      messages.length = 0
      await monitorRequested(deps(hostnameId))(action(hostnameId, {
        clusterId, monitorAction: monitorCase.monitorAction, targetNodeIds: [NODE_ID],
      }))
      assert.equal(messages.length, 1, "the requester receives one response")
      const broadcastCount = monitorCase.monitorAction === "status" ? 0 : 1
      assert.equal(aliasMessages.length, broadcastCount)
      assert.equal(sharedMessages.length, broadcastCount)
      assert.equal(unrelatedMessages.length, 0)
      for (const message of [...messages, ...aliasMessages, ...sharedMessages]) {
        const reply = JSON.parse(message)
        assert.equal(reply.type, VALKEY.MONITOR.monitorFulfilled)
        assert.equal(reply.payload.nodeId, NODE_ID)
        assert.equal(reply.payload.clusterId, clusterId)
        assert.deepEqual(reply.payload.parsedResponse, response)
      }
    })
  }

  const consumers = [
    { name: "Dashboard", handler: setData, response: { info: {}, memory: {} }, type: VALKEY.STATS.setData },
    { name: "CPU", handler: cpuUsageRequested, response: [], type: VALKEY.CPU.cpuUsageFulfilled },
    { name: "memory", handler: memoryUsageRequested, response: {}, type: VALKEY.MEMORY.memoryUsageFulfilled },
    { name: "Monitor", handler: monitorRequested, response: { monitorRunning: false, checkAt: null, startedAt: null },
      type: VALKEY.MONITOR.monitorFulfilled, payload: { monitorAction: "status" } },
    { name: "hot keys", handler: hotKeysRequested, response: { nodeId: NODE_ID, hotKeys: [] },
      type: VALKEY.HOTKEYS.hotKeysFulfilled },
    { name: "big keys", handler: bigKeysRequested, response: { nodeId: NODE_ID, keys: [] },
      type: VALKEY.BIGKEYS.bigKeysFulfilled },
    { name: "command logs", handler: commandLogsRequested, response: { nodeId: NODE_ID, rows: [] },
      type: VALKEY.COMMANDLOGS.commandLogsFulfilled, payload: { commandLogType: COMMANDLOG_TYPE.SLOW } },
    { name: "config", handler: runConfigPushSession, response: { success: true, message: "saved" },
      type: VALKEY.CONFIG.updateConfigFulfilled, payload: { config: {} } },
  ]

  for (const consumer of consumers) {
    it(`${consumer.name} uses the canonical collector and preserves the reply's UI identity`, async () => {
      const id = await connect()
      metricsServerMap.get(NODE_ID)!.metricsURI = "http://127.0.0.1:9001"
      const fetch = mock.method(globalThis, "fetch", async () => Response.json(consumer.response))
      const lookup = mock.method(dns, "lookup", async () => { throw new Error("must not resolve DNS on reads") })
      messages.length = 0
      await consumer.handler(deps(id))(action(id, consumer.payload))
      assert.equal(fetch.mock.calls.length, 1)
      assert.ok(String(fetch.mock.calls[0].arguments[0]).startsWith("http://127.0.0.1:9001/"))
      assert.equal(lookup.mock.calls.length, 0)
      const reply = messages.map((message) => JSON.parse(message)).find((message) => message.type === consumer.type)
      assert.ok(reply)
      assert.equal(reply.payload.connectionId ?? reply.payload.nodeId, reply.payload.connectionId ? id : toNodeId(id))
    })
  }

  it("reports an unavailable collector through the existing controlled Dashboard error", async () => {
    const id = await connect()
    metricsServerMap.delete(NODE_ID)
    const fetch = mock.method(globalThis, "fetch", async () => { throw new Error("must not fetch") })
    messages.length = 0
    await setData(deps(id))(action(id))
    assert.equal(fetch.mock.calls.length, 0)
    const reply = JSON.parse(messages[0])
    assert.equal(reply.type, VALKEY.STATS.setError)
    assert.equal(reply.payload.errorKind, METRICS_SERVER_NOT_READY)
  })
})
