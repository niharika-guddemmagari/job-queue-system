
"""
JACKFRUIT - Ryu SDN Controller API Client

Reads actual switch and OpenFlow information from Ryu's
ofctl_rest API. This module does not simulate network events.
"""

import json
import urllib.error
import urllib.request


RYU_HOST = "127.0.0.1"
RYU_PORT = 8081
TIMEOUT = 3


def api_get(path):
    url = f"http://{RYU_HOST}:{RYU_PORT}{path}"

    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json"}
    )

    try:
        with urllib.request.urlopen(
            request, timeout=TIMEOUT
        ) as response:
            return json.loads(response.read().decode())

    except (
        urllib.error.URLError,
        urllib.error.HTTPError,
        TimeoutError,
        json.JSONDecodeError
    ) as error:
        return {
            "error": str(error)
        }


def get_switches():
    """Return datapath IDs connected to Ryu."""

    result = api_get("/stats/switches")

    if isinstance(result, dict):
        return []

    return result


def get_switch_description(dpid):
    return api_get(f"/stats/desc/{dpid}")


def get_switch_flows(dpid):
    """Return actual OpenFlow entries for a switch."""

    result = api_get(f"/stats/flow/{dpid}")

    if isinstance(result, dict) and "error" in result:
        return []

    return result.get(str(dpid), [])


def get_switch_ports(dpid):
    """Return port statistics reported by the switch."""

    result = api_get(f"/stats/port/{dpid}")

    if isinstance(result, dict) and "error" in result:
        return []

    return result.get(str(dpid), [])


def get_topology_links():
    """Return links discovered by Ryu's topology service."""

    return api_get("/v1.0/topology/links")


def get_topology_switches():
    return api_get("/v1.0/topology/switches")


def get_controller_status():
    """Collect a dashboard-friendly snapshot."""

    switches = get_switches()

    if not switches:
        return {
            "connected": False,
            "switch_count": 0,
            "switches": [],
            "links": [],
            "error": "No switches reported by Ryu"
        }

    switch_data = []

    for dpid in switches:
        flows = get_switch_flows(dpid)
        ports = get_switch_ports(dpid)

        switch_data.append({
            "dpid": str(dpid),
            "flow_count": len(flows),
            "flows": flows,
            "ports": ports
        })

    links = get_topology_links()

    if isinstance(links, dict) and "error" in links:
        links = []

    return {
        "connected": True,
        "switch_count": len(switches),
        "switches": switch_data,
        "links": links
    }


if __name__ == "__main__":
    print(json.dumps(get_controller_status(), indent=2))

