#!/usr/bin/env python
import hashlib
import socket
ipopVerMjr = "16"
ipopVerMnr = "01"
ipopVerRev = "0"
ipopVerRel = "{0}.{1}.{2}".format(ipopVerMjr, ipopVerMnr, ipopVerRev)

# set default config values
CONFIG = {
    "CFx": {
        "local_uid": "",  # Attribute to store node UID needed by Statreport and SVPN
        "uid_size": 40,  # No of bytes for node UID
        "router_mode": False,
        "ipopVerRel": ipopVerRel,
    },
    "VirtualNetworkInitializer": {
        "Enabled": True,
        "MTU4": 1200,   # Default MTU for IPv4 network
        "MTU6": 1200,   # Default MTU for IPv6 network
        "LocalPrefix6": 64,    # IPv6 prefix
        "LocalPrefix4": 16,     # IPV4 Prefix
        "dependencies": ["Logger", "TincanInterface"]
    },
    "TincanInterface": {
        "buf_size": 65507,      # Max buffer size for Tincan Messages
        "SocketReadWaitTime": 15,   # Socket read wait time for Tincan Messages
        "ctrl_recv_port": 5801,     # Controller UDP Listening Port
        "ip6_prefix": "fd50:0dbc:41f2:4a3c",
        "localhost": "127.0.0.1",
        "ctrl_send_port": 5800,     # Tincan UDP Listening Port
        "localhost6": "::1",
        "dependencies": ["Logger"]
    },
    "LinkManager": {
        "Enabled": True,
        "TimerInterval": 10,                # Timer thread interval in sec
        "InitialLinkTTL": 120,              # Initial Time to Live for a p2p link in sec
        "LinkPulse": 180,                   # Time to Live for an online p2p link in sec
        "MaxConnRetry": 5,                  # Max Connection Retry attempts for each p2p link
        "dependencies": ["Logger", "VirtualNetworkInitializer", "TincanInterface", "BaseTopologyManager"]
    },
    "BroadCastForwarder": {
        "Enabled": True,
        "TimerInterval": 5,                # Timer thread interval in sec
        "dependencies": ["Logger", "VirtualNetworkInitializer", "TincanInterface"]
    },
    "UnmanagedNodeDiscovery": {
        "Enabled": True,
        "dependencies": ["Logger", "VirtualNetworkInitializer", "TincanInterface", "LinkManager"]
    },
    "IPMulticast": {
        "Enabled": True,
        "dependencies": ["Logger", "VirtualNetworkInitializer", "TincanInterface"]
    },
    "XmppClient": {
        "Enabled": True,
        "MessagePerIntervalDelay": 10,      # No of XMPP messages after which the delay has to be increased
        "InitialAdvertismentDelay": 5,      # Initial delay for Peer XMPP messages
        "XmppAdvrtDelay": 5,                # Incremental delay for XMPP messages
        "MaxAdvertismentDelay": 30,         # Max XMPP Message delay
        "dependencies": ["Logger", "VirtualNetworkInitializer", "TincanInterface"]
    },
    "BaseTopologyManager": {
        "Enabled": True,
        "TimerInterval": 15,            # Timer thread interval in sec
        "NumberOfSuccessors": 2,        # Max number of successor links
        "NumberOfChords": 0,            # Max number of chord links
        "NumberOfOnDemand": 0,          # Max number of Ondemand Links
        "NumberOfInbound": 20,          # Max number of Inbound links
        "OnDemandLinkTTL": 60,          # Time to Live for an Ondemand Link
        "OndemandThreshold": 1000,      # No of messages after which an Ondemand link would be created
        "OndemandConnectionWaitTime": 15,   # Wait time between each Ondemand Link creation
        "dependencies": ["Logger", "VirtualNetworkInitializer", "TincanInterface", "XmppClient"]
    },
    "OverlayVisualizer": {
        "Enabled": False,           # Set this field to True for sending data to the visualizer
        "WebServiceAddress": ":8080/insertdata",    # Visualizer webservice URL
        "TopologyDataQueryInterval": 5,             # Interval to query TopologyManager to get network stats
        "WebServiceDataPostInterval": 5,            # Interval to send data to the visualizer
        "TimerInterval": 1,                         # Timer thread interval
        "NodeName": "",                             # Node Name as seen from the UI
        "dependencies": ["Logger"]
    }
}


def gen_ip6(uid, ip6=None):
    if ip6 is None:
        ip6 = CONFIG["TincanInterface"]["ip6_prefix"]
    for i in range(0, 16, 4):
        ip6 += ":" + uid[i:i+4]
    return ip6


# Generates UID from IPv4
def gen_uid(ip4):
    return hashlib.sha1(ip4.encode('utf-8')).hexdigest()[:CONFIG["CFx"]["uid_size"]]


# Function to send UDP message to Tincan
def send_msg(sock, msg):
    if socket.has_ipv6:
        dest = (CONFIG["TincanInterface"]["localhost6"],
                CONFIG["TincanInterface"]["ctrl_send_port"])
    else:
        dest = (CONFIG["TincanInterface"]["localhost"],
                CONFIG["TincanInterface"]["ctrl_send_port"])
    return sock.sendto(bytes(msg.encode('utf-8')), dest)
