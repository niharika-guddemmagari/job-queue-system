
"""
JACKFRUIT - Multi-Switch Mininet Topology

Hosts:
h1: Manager + Client 1
h2: Worker 1
h3: Worker 2
h4: Client 2
h5: Client 3
h6: Client 4

Switches:
s1: Manager-side switch
s2: Worker-side switch
s3: Client-side switch
s4: Redundant-path switch
"""

from functools import partial

from mininet.net import Mininet
from mininet.topo import Topo
from mininet.node import RemoteController, OVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel, info


class JackfruitTopo(Topo):

    def build(self):

        # Hosts
        h1 = self.addHost("h1", ip="10.0.0.1/24")
        h2 = self.addHost("h2", ip="10.0.0.2/24")
        h3 = self.addHost("h3", ip="10.0.0.3/24")
        h4 = self.addHost("h4", ip="10.0.0.4/24")
        h5 = self.addHost("h5", ip="10.0.0.5/24")
        h6 = self.addHost("h6", ip="10.0.0.6/24")

        # Switches
        s1 = self.addSwitch("s1")
        s2 = self.addSwitch("s2")
        s3 = self.addSwitch("s3")
        s4 = self.addSwitch("s4")

        # Manager and client connections
        self.addLink(h1, s1)
        self.addLink(h4, s1)

        # Worker connections
        self.addLink(h2, s2)
        self.addLink(h3, s2)

        # Additional client connections
        self.addLink(h5, s3)
        self.addLink(h6, s3)

        # Inter-switch links
        self.addLink(s1, s2)
        self.addLink(s1, s3)
        self.addLink(s2, s4)
        self.addLink(s3, s4)


def run():
    topo = JackfruitTopo()

    net = Mininet(
        topo=topo,
        controller=None,
        switch=partial(OVSSwitch, stp=True),
        autoSetMacs=True,
        waitConnected=False
    )

    net.addController(
        "c0",
        controller=RemoteController,
        ip="127.0.0.1",
        port=6633
    )

    info("*** Starting Jackfruit multi-switch network\n")
    net.start()

    info("*** Network is running\n")
    info("*** Test connectivity with: pingall\n")
    info("*** Hosts: h1 h2 h3 h4 h5 h6\n")

    CLI(net)
    net.stop()


if __name__ == "__main__":
    setLogLevel("info")
    run()

