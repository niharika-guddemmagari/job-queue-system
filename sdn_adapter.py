
"""
JACKFRUIT - SDN Adapter
Installs and inspects OpenFlow rules using ovs-ofctl.
"""

import ipaddress
import subprocess


SWITCH = "s1"
OPENFLOW_VERSION = "OpenFlow13"


def run_ovs(command, timeout=5):
    """Execute an OVS command and return its output."""

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False
        )

        if result.returncode != 0:
            print("[SDN ERROR]", result.stderr.strip())
            return None

        return result.stdout.strip()

    except (subprocess.TimeoutExpired, FileNotFoundError) as error:
        print("[SDN ERROR]", error)
        return None


def install_worker_flow(worker_ip, switch_port):
    """
    Install a destination-IP forwarding rule for a worker.

    switch_port must be the actual worker-facing port on s1.
    """

    try:
        ipaddress.ip_address(worker_ip)
        switch_port = int(switch_port)

        if not 1 <= switch_port <= 65535:
            return False

    except (ValueError, TypeError):
        print("[SDN] Invalid worker IP or switch port")
        return False

    flow = (
        f"priority=100,ip,nw_dst={worker_ip},"
        f"actions=output:{switch_port}"
    )

    output = run_ovs([
        "ovs-ofctl",
        "-O", OPENFLOW_VERSION,
        "add-flow",
        SWITCH,
        flow
    ])

    if output is None:
        return False

    print(
        f"[SDN] Installed route: {worker_ip} "
        f"-> {SWITCH} port {switch_port}"
    )

    return True


def delete_worker_flow(worker_ip):
    """Delete the forwarding rule for a worker."""

    try:
        ipaddress.ip_address(worker_ip)
    except ValueError:
        return False

    output = run_ovs([
        "ovs-ofctl",
        "-O", OPENFLOW_VERSION,
        "del-flows",
        SWITCH,
        f"ip,nw_dst={worker_ip}"
    ])

    return output is not None


def get_switch_flows():
    """Read actual OpenFlow rules and their packet/byte counters."""

    output = run_ovs([
        "ovs-ofctl",
        "-O", OPENFLOW_VERSION,
        "dump-flows",
        SWITCH
    ])

    if output is None:
        return []

    flows = []

    for line in output.splitlines():
        if "actions=" not in line:
            continue

        flows.append(line.strip())

    return flows


def get_flow_stats():
    """Extract packet and byte counters from installed flows."""

    flows = get_switch_flows()
    stats = []

    for line in flows:
        entry = {
            "raw": line,
            "packets": 0,
            "bytes": 0,
            "destination": None,
            "actions": None
        }

        for field in line.split(","):
            field = field.strip()

            if field.startswith("n_packets="):
                try:
                    entry["packets"] = int(
                        field.split("=", 1)[1]
                    )
                except ValueError:
                    pass

            elif field.startswith("n_bytes="):
                try:
                    entry["bytes"] = int(
                        field.split("=", 1)[1]
                    )
                except ValueError:
                    pass

            elif field.startswith("nw_dst="):
                entry["destination"] = field.split(
                    "=", 1
                )[1]

            elif field.startswith("actions="):
                entry["actions"] = field.split(
                    "=", 1
                )[1]

        stats.append(entry)

    return stats


def get_sdn_status():
    """Return switch and flow information for the dashboard."""

    flows = get_flow_stats()

    return {
        "switch": SWITCH,
        "connected": bool(flows),
        "flow_count": len(flows),
        "total_packets": sum(
            flow["packets"] for flow in flows
        ),
        "total_bytes": sum(
            flow["bytes"] for flow in flows
        ),
        "flows": flows
    }

