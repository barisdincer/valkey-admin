import { getMetricsNodeId } from "../connection"
import { type Deps, withDeps } from "./utils"
import { setDashboardData } from "../set-dashboard-data"

export const setData = withDeps<Deps, void>(
  async ({ ws, metricsServerMap, connectionId, clients }) => {
    const metricsServerURI = metricsServerMap.get(getMetricsNodeId(connectionId, clients))?.metricsURI
    await setDashboardData(connectionId, metricsServerURI, ws)
  },
)
