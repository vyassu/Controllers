#!/usr/bin/env python
import hashlib
import json,socket
import logging
ipopVerMjr = "16";
ipopVerMnr = "01";
ipopVerRev = "0";
ipopVerRel = "{0}.{1}.{2}".format(ipopVerMjr, ipopVerMnr, ipopVerRev)

# set default config values
CONFIG = {
    "CFx": {
        "local_uid": "",
        "uid_size": 40,
        "router_mode": False,
        "ipopVerRel" : ipopVerRel,
    },
    "VirtualNetworkInitializer":{
        "Enabled": True,
        "MTU4": 1200,
        "MTU6": 1200,
        "LocalPrefix6": 64,
        "LocalPrefix4": 16,
        "dependencies": [ "Logger", "TincanInterface" ]
    },
    "TincanInterface": {
        "buf_size": 65507,
        "SocketReadWaitTime": 15,
        "ctrl_recv_port": 5801,
        "ip6_prefix": "fd50:0dbc:41f2:4a3c",
        "localhost": "127.0.0.1",
        "ctrl_send_port": 5800,
        "localhost6": "::1",
        "dependencies": ["Logger"]
    },
    "BroadCastForwarder": {
        "Enabled": True,
        "dependencies": [ "Logger" ]
    },
    "NodeDiscovery": {
        "Enabled": True,
        "dependencies": [ "Logger" ]
    },
    "IPMulticast": {
        "Enabled": True,
        "dependencies": ["Logger"]
    },
    "XmppClient": {
        "Enabled": True,
        "dependencies": [ "Logger","VirtualNetworkInitializer","TincanInterface" ]
    },
    "ConnectionManager": {
        "Enabled": True,
        "TimerInterval": 5,
        "InitialLinkTTL": 120,
        "ChordLinkTTL": 180,
        "LinkPulse": 180,
        "OndemandLinkRateThreshold": 128,
        "MaxConnRetry": 5,
        "dependencies": ["Logger"]
    },
    "BaseTopologyManager": {
        "Enabled": True,
        "NumberOfSuccessors": 2,
        "NumberOfChords": 0,
        "NumberOfOnDemand": 0,
        "NumberOfInbound": 20,
        "InitialLinkTTL": 120,
        "LinkPulse": 180,
        "OnDemandLinkTTL": 60,
        "TimerInterval": 1,
        "TopologyRefreshInterval": 15,
        "OndemandThreshold":1000,
        "OndemandConnectionWaitTime": 15,
        "NumberOfPingsToPeer": 5,
        "PeerPingInterval": 300,
        "MaxConnRetry": 5,
        "dependencies": [ "Logger" ]
    },
    "OverlayVisualizer": {
        "Enabled": False,
        "WebServiceAddress": ":8080/insertdata",
        "TopologyDataQueryInterval": 5,
        "WebServiceDataPostInterval": 5,
        "TimerInterval": 5,
        "NodeName": "",
        "dependencies": [ "Logger" ]
    }
}

def gen_ip6(uid, ip6=None):
    if ip6 is None:
        ip6 = CONFIG["TincanInterface"]["ip6_prefix"]
    for i in range(0, 16, 4):
        ip6 += ":" + uid[i:i+4]
    return ip6

def gen_uid(ip4):
    return hashlib.sha1(ip4.encode('utf-8')).hexdigest()[:CONFIG["CFx"]["uid_size"]]

def send_msg(sock, msg):
    if socket.has_ipv6:
        dest = (CONFIG["TincanInterface"]["localhost6"],
                CONFIG["TincanInterface"]["svpn_port"])
    else:
        dest = (CONFIG["TincanInterface"]["localhost"],
                CONFIG["TincanInterface"]["svpn_port"])
    return sock.sendto(bytes((msg).encode('utf-8')),dest)
