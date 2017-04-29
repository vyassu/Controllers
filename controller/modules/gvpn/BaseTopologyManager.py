from controller.framework.ControllerModule import ControllerModule
from controller.framework.CFx import CFX
import time
import math


class BaseTopologyManager(ControllerModule, CFX):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(BaseTopologyManager, self).__init__(CFxHandle, paramDict, ModuleName)
        self.CFxHandle = CFxHandle
        self.CMConfig = paramDict
        self.interval_counter = 0
        self.ipop_interface_details = {}
        self.max_num_links = self.CMConfig["NumberOfSuccessors"] + self.CMConfig["NumberOfChords"] + \
                             self.CMConfig["NumberOfOnDemand"] + self.CMConfig["NumberOfInbound"]

        self.tincanparams = self.CFxHandle.queryParam("VirtualNetworkInitializer", "Vnets")
        for k in range(len(self.tincanparams)):
            interface_name = self.tincanparams[k]["TapName"]
            self.ipop_interface_details[interface_name] = {}
            interface_details = self.ipop_interface_details[interface_name]
            interface_details["p2p_state"] = "started"
            interface_details["GeoIP"] = ""
            interface_details["ipop_state"] = {}
            interface_details["discovered_nodes"] = []
            interface_details["UpdateXMPPPeerFlag"] = False
            interface_details["log_chords"] = []
            interface_details["successor"] = {}
            interface_details["chord"] = {}
            interface_details["on_demand"] = {}
            interface_details["ip_uid_table"] = {}
            interface_details["uid_mac_table"] = {}
            interface_details["mac_uid_table"] = {}
            interface_details["peer_uid_sendmsgcount"] = {}
            interface_details["xmpp_client_code"] = self.tincanparams[k]["XMPPModuleName"]
        self.tincanparams = None

    def initialize(self):
        # Get Peer Nodes from XMPP server
        for interface_name in self.ipop_interface_details.keys():
            self.registerCBT('TincanInterface', 'DO_GET_STATE', {"interface_name": interface_name, "MAC": ""})
            self.ipop_interface_details[interface_name]["GeoIP"] = self.getGeoIP()
            self.registerCBT(self.ipop_interface_details[interface_name]["xmpp_client_code"], "GetXMPPPeerList",
                             {"interface_name": interface_name})
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def terminate(self):
        pass

    # remove connection
    # remove a link by peer UID
    def remove_link(self, uid, interface_name, connection_type=None):
        if connection_type is None:
            connection_type_list = ["successor", "chord", "on_demand"]
        else:
            connection_type_list = [connection_type]

        for con_type in connection_type_list:
            if uid in self.ipop_interface_details[interface_name][con_type].keys():
                self.ipop_interface_details[interface_name][con_type].pop(uid)
                message = {"uid": uid, "interface_name": interface_name}
                self.registerCBT("ConnectionManager", "REMOVE_CONNECTION", message)
                log = "Connection remove request for UID: {0}".format(uid)
                self.registerCBT('Logger', 'info', log)

############################################################################
        # successors policy                                                        #
############################################################################
        # [1] A discovers nodes in the network
        #     A requests to link to the closest successive node B as A's successor
        # [2] B accepts A's link request, with A as B's inbound link
        #     B responds to link to A
        # [3] A and B are connected
        # [*] the link is terminated when A discovers and links to closer successive
        #     nodes, or the link disconnects

    def add_successors(self, interface_name):
        # sort nodes into rotary, unique list with respect to this UID
        interface_details = self.ipop_interface_details[interface_name]
        uid = interface_details["ipop_state"]["_uid"]
        nodes = list(sorted(interface_details["discovered_nodes"]))

        if uid in nodes:
            nodes.remove(uid)
        if max([uid] + nodes) != uid:
            while nodes[0] < uid:
                nodes.append(nodes.pop(0))

        requested_nodes = []
        # link to the closest <num_successors> nodes (if not already linked)
        for node in nodes[0:min(len(nodes), self.CMConfig["NumberOfSuccessors"])]:
            if node not in interface_details["successor"].keys():
                self.add_outbound_link("successor", node, interface_name)
                requested_nodes.append(node)

        # establishing link from the smallest UID node in the network to the biggest UID in the network
        if min([uid] + nodes) == uid and len(nodes) > 1:
            for node in list(reversed(nodes))[0:self.CMConfig["NumberOfSuccessors"]]:
                if node not in interface_details["successor"].keys():
                    self.add_outbound_link("successor", node, interface_name)
                    requested_nodes.append(node)

    def remove_successors(self, interface_name):
        # sort nodes into rotary, unique list with respect to this UID
        interface_details = self.ipop_interface_details[interface_name]
        successors = list(sorted(interface_details["successor"].keys()))
        local_uid = interface_details["ipop_state"]["_uid"]

        # Allow the least node in the network to have the as many connections as required to maintain a fully connected
        # Ring Topology
        if max([local_uid] + successors) != local_uid:
            while successors[0] < local_uid:
                successors.append(successors.pop(0))

        # remove all linked successors not within the closest <num_successors> linked nodes
        # remove all unlinked successors not within the closest <num_successors> nodes
        num_linked_successors = 0

        if len(successors) % 2 == 0:
            loop_counter = len(successors) / 2
        elif len(successors) == 1:
            loop_counter = 0
        else:
            loop_counter = len(successors) / 2 + 1

        i = 0
        while i < loop_counter:
            if successors[i] not in successors:
                num_linked_successors += 1
                if num_linked_successors > (2 * int(self.CMConfig["num_successors"])):
                    self.remove_link(successors[i], interface_name, connection_type="successor")
            if successors[-(i + 1)] not in successors:
                num_linked_successors += 1
                if num_linked_successors > (2 * int(self.CMConfig["num_successors"])):
                    self.remove_link(successors[-(i + 1)], interface_name, connection_type="successor")
            i += 1

############################################################################
            # chords policy                                                            #
############################################################################
    # [1] A forwards a headless find_chord message approximated by a designated UID
    # [2] B discovers that it is the closest node to the designated UID
    #     B responds with a found_chord message to A
    # [3] A requests to link to B as A's chord
    # [4] B accepts A's link request, with A as B's inbound link
    #     B responds to link to A
    # [5] A and B are connected
    # [*] the link is terminated when the chord time-to-live attribute expires and
    #     a better chord was found or the link disconnects

    def find_chords(self, interface_name):
        # find chords closest to the approximate logarithmic nodes
        link_details = self.ipop_interface_details[interface_name]
        current_node_uid = link_details["ipop_state"]["_uid"]
        if len(link_details["log_chords"]) == 0:
            for i in reversed(range(self.CMConfig["NumberOfChords"])):
                log_num = (int(current_node_uid, 16) + int(math.pow(2, 160 - 1 - i))) % int(math.pow(2, 160))
                log_uid = "{0:040x}".format(log_num)
                link_details["log_chords"].append(log_uid)

        # determine list of designated UIDs
        log_chords = link_details["log_chords"]
        for chord in link_details["chord"].values():
            if "log_uid" in chord.keys():
                if chord["log_uid"] in log_chords:
                    log_chords.remove(chord["log_uid"])

        # forward find_chord messages to the nodes closest to the designated UID
        for log_uid in log_chords:
            # forward find_chord message
            new_msg = {
                        "fwd_type": "closest",
                        "dst_uid": log_uid,
                        "interface_name": interface_name,
                        "data": {
                            "msg_type": "find_chord",
                            "src_uid": current_node_uid,
                            "dst_uid": log_uid,
                            "log_uid": log_uid
                        }
            }

            self.registerCBT("BaseTopologyManager", "forward_msg", new_msg)

    # Gets GEO Location IP (needed by Visualizer Module)
    def getGeoIP(self):
        try:
            # TO DO implement GEOIP from CAS
            # stun_details = self.CFxHandle.queryParam("VirtualNetworkInitializer", "Stun")[0].split(":")
            # nat_type, external_ip, external_port = stun.get_ip_info(stun_host=stun_details[0],stun_port=int(stun_details[1]))
            # return external_ip
            return ""
        except Exception as err:
            self.registerCBT("Logger", "error", "Exception caught retrieving GeoIP:{0}".format(err))
            return ""

    def clean_chord(self, interface_name):
        links = self.ipop_interface_details[interface_name]
        if not links["chord"].keys():
            return

        # find chord with the oldest time-to-live attribute
        uid = min(links["chord"].keys(), key=lambda u: (links["chord"][u]["ttl"]))

        # time-to-live attribute has expired: determine if a better chord exists
        if time.time() > links["chord"][uid]["ttl"]:
            # forward find_chord message
            if "log_uid" in links["chord"][uid].keys():
                new_msg = {
                    "msg_type": "find_chord",
                    "src_uid": self.ipop_interface_details[interface_name]["uid"],
                    "dst_uid": links["chord"][uid]["log_uid"],
                    "log_uid": links["chord"][uid]["log_uid"]
                }
                forward_message = {
                    "fwd_type": "closest",
                    "dst_uid": links["chord"][uid]["log_uid"],
                    "interface_name": interface_name,
                    "data": new_msg
                }
                self.registerCBT("BaseTopologyManager", "forward_msg", forward_message)

                # extend time-to-live attribute
                links["chord"][uid]["ttl"] = time.time() + self.CMConfig["ttl_chord"]

    def add_outbound_link(self, con_type, uid, interface_name):
        self.registerCBT("ConnectionManager", "REQUEST_CONNECTION", {"uid": uid,
                                                                     "interface_name": interface_name,
                                                                     "con_type": con_type})

    def processCBT(self, cbt):
        msg = cbt.data
        if cbt.action == "UpdateXMPPPeerList":
            interface_name = msg["interface_name"]
            xmpp_peer_list = msg.get("peer_list")
            if len(xmpp_peer_list) > 0:
                self.ipop_interface_details[interface_name]["discovered_nodes"] += xmpp_peer_list
                self.ipop_interface_details[interface_name]["discovered_nodes"] = \
                    list(set(self.ipop_interface_details[interface_name]["discovered_nodes"]))
            else:
                self.ipop_interface_details[interface_name]["discovered_nodes"] = []
            self.registerCBT(self.ipop_interface_details[interface_name]["xmpp_client_code"], "GetXMPPPeerList",
                             {"interface_name": interface_name})

        elif cbt.action == "forward_msg":
                msg = cbt.data
                self.forward_msg(msg["fwd_type"], msg["dst_uid"], msg["data"], msg["interface_name"])

        elif cbt.action == "UpdateConnectionDetails":
                interface_name = msg.get("interface_name")
                msg_type = msg["msg_type"]
                uid = msg.get("uid")
                mac = msg.get("mac")
                con_type = msg.get("con_type")

                if msg_type == "add_peer":
                    if uid not in self.ipop_interface_details[interface_name][con_type]:
                        self.ipop_interface_details[interface_name][con_type][uid] = {
                            "uid": uid,
                            "mac": mac,
                            "ttl": time.time()+self.CMConfig["InitialLinkTTL"],
                            "status": msg.get("status", "offline")
                        }

                    self.ipop_interface_details[interface_name]["uid_mac_table"][uid] = [mac]
                    self.ipop_interface_details[interface_name]["mac_uid_table"][mac] = uid
                    self.registerCBT('Logger', 'info', "**Peer UID {0} added to BTM Table as {1}**".format(uid, con_type))
                elif msg_type == "remove_peer":
                    if uid in list(self.ipop_interface_details[interface_name]["successor"].keys()):
                        del self.ipop_interface_details[interface_name]["successor"][uid]
                    if uid in list(self.ipop_interface_details[interface_name]["chord"].keys()):
                        del self.ipop_interface_details[interface_name]["chord"][uid]
                    if uid in list(self.ipop_interface_details[interface_name]["on_demand"].keys()):
                        del self.ipop_interface_details[interface_name]["on_demand"][uid]
                    if uid in list(self.ipop_interface_details[interface_name]["uid_mac_table"].keys()):
                        maclist = list(self.ipop_interface_details[interface_name]["uid_mac_table"][uid])
                        for mac in maclist:
                            del self.ipop_interface_details[interface_name]["mac_uid_table"][mac]
                        del self.ipop_interface_details[interface_name]["uid_mac_table"][uid]
                    self.registerCBT('Logger', 'info', "**Peer UID {0} removed from BTM**".format(uid))
                else:
                    log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                        .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
                    self.registerCBT('Logger', 'warning', log)

        elif cbt.action == "XMPP_MSG":
                msg = cbt.data
                msg_type = msg.get("type", None)
                interface_name = msg["interface_name"]
                interface_details = self.ipop_interface_details[interface_name]
                # Remove Offline peer node
                if msg_type == "offline_peer":
                    if msg["uid"] in interface_details["discovered_nodes"]:
                        interface_details["discovered_nodes"].remove(msg["uid"])
                    log = "Removed peer from discovered node list {0}".format(msg["uid"])
                    self.registerCBT('Logger', 'debug', log)
                    self.registerCBT("ConnectionManager", "REMOVE_CONNECTION", {"uid": msg["uid"], "interface_name":
                        interface_name })
                else:
                    log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                        .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
                    self.registerCBT('Logger', 'warning', log)

        elif cbt.action == "TINCAN_CONTROL":
                msg = cbt.data
                msg_type = msg.get("type", None)
                interface_name = msg["interface_name"]
                interface_details = self.ipop_interface_details[interface_name]
                # update local state
                if msg_type == "local_state":
                    interface_details["ipop_state"] = msg
                    interface_details["mac"] = msg["mac"]
                    interface_details["mac_uid_table"][msg["mac"]] = msg["_uid"]
                    if msg["_uid"] not in interface_details["uid_mac_table"].keys():
                        interface_details["uid_mac_table"][msg["_uid"]] = [msg["mac"]]
                    self.registerCBT("Logger", "info", "Local Node Info UID:{0} MAC:{1} IP4: {2}".format(msg["_uid"],
                                                                                                         msg["mac"],
                                                                                                         msg["_ip4"]))
                # update peer list
                elif msg_type == "peer_state":
                    uid = msg["uid"]
                    conn_type = msg["con_type"]
                    interface_details["mac_uid_table"][msg["mac"]] = uid

                    # Creating an entry for the peer in the UID_MAC_Table
                    if uid not in interface_details["uid_mac_table"].keys():
                        if "unknown" != msg["status"]:
                            interface_details["uid_mac_table"][msg["uid"]] = [msg["mac"]]

                    # Creating an entry in the IP-UID Table
                    if msg["ip4"] not in interface_details["ip_uid_table"].keys():
                        if "unknown" != msg["status"]:
                            interface_details["ip_uid_table"][msg["ip4"]] = uid

                    # check whether UID exits in link_type
                    if uid in interface_details[conn_type].keys():
                        # preserve ttl and con_status attributes
                        if "ttl" not in interface_details[conn_type][uid]:
                            interface_details[conn_type][uid]["ttl"] = time.time()
                        ttl = interface_details[conn_type][uid]["ttl"]
                        # update ttl attribute
                        if "online" == msg["status"]:
                            ttl = time.time() + self.CMConfig["LinkPulse"]
                        elif "unknown" == msg["status"]:
                            if uid in list(self.ipop_interface_details[interface_name]["successor"].keys()):
                                del self.ipop_interface_details[interface_name]["successor"][uid]
                            if uid in list(self.ipop_interface_details[interface_name]["chord"].keys()):
                                del self.ipop_interface_details[interface_name]["chord"][uid]
                            if uid in list(self.ipop_interface_details[interface_name]["on_demand"].keys()):
                                del self.ipop_interface_details[interface_name]["on_demand"][uid]

                            if uid in list(interface_details["uid_mac_table"].keys()):
                                mac_list = list(interface_details["uid_mac_table"][uid])
                                for mac in mac_list:
                                    del interface_details["mac_uid_table"][mac]
                                del interface_details["uid_mac_table"][uid]
                            return

                        # update peer state within BTM Tables
                        interface_details[conn_type][uid]["ttl"] = ttl
                        interface_details[conn_type][uid]["status"] = msg["status"]
                        # Send connection details to Conn Manager
                        message = {
                            "uid": msg["uid"],
                            "stats": msg["stats"],
                            "status": msg["status"],
                            "mac": msg["mac"],
                            "interface_name": interface_name
                        }
                        self.registerCBT("ConnectionManager", "UPDATE_CONNECTION", message)
                elif msg_type == "UpdateMACUIDIp":
                    location = msg.get("location")
                    uid = msg["uid"]
                    localuid = interface_details["ipop_state"]["_uid"]

                    # check whether an entry exists for UID
                    if uid not in list(interface_details["uid_mac_table"].keys()):
                        interface_details["uid_mac_table"][uid] = []

                    self.registerCBT('Logger', 'debug', 'UpdateMACUIDMessage:::' + str(msg))
                    '''
                    if uid not in interface_details["online_peer_uid"] and uid != localuid:
                        nextuid = self.getnearestnode(uid, interface_name)
                        nextnodemac = interface_details["peers"][nextuid]["mac"]

                        
                        for destmac in list(msg["mac_ip_table"].keys()):
                            self.registerCBT('Logger', 'info', 'MAC_UID Table:::' + str(interface_details["mac_uid_table"]))

                            if destmac not in list(interface_details["mac_uid_table"].keys()):
                                message = {
                                    "interface_name": interface_name,
                                    "sourcemac": nextnodemac,
                                    "destmac": [destmac]
                                }
                                self.registerCBT("TincanInterface", "DO_INSERT_ROUTING_RULES", message)
                            else:
                                olduid = interface_details["mac_uid_table"][destmac]
                                if olduid != uid:
                                    message = {
                                        "interface_name": interface_name,
                                        "sourcemac": nextnodemac,
                                        "destmac": [destmac]
                                    }
                                    self.registerCBT("TincanInterface", "DO_INSERT_ROUTING_RULES", message)
                        '''

                    for mac, ip in msg["mac_ip_table"].items():
                        if mac not in interface_details["uid_mac_table"][uid]:
                            interface_details["uid_mac_table"][uid].append(mac)
                        interface_details["ip_uid_table"].update({ip: uid})
                        interface_details["mac_uid_table"].update({mac: uid})

                else:
                    log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                        .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
                    self.registerCBT('Logger', 'warning', log)

        elif cbt.action == "ICC_CONTROL":
                msg = cbt.data
                msg_type = msg.get("msg_type", None)
                interface_name = msg["interface_name"]
                # advertisement of nearby nodes
                if msg_type == "advertise":
                    self.ipop_interface_details[interface_name]["discovered_nodes"] \
                        = list(set(self.ipop_interface_details[interface_name]["discovered_nodes"] + msg["peer_list"]))
                    localuid = self.ipop_interface_details[interface_name]["ipop_state"]["_uid"]
                    if localuid in self.ipop_interface_details[interface_name]["discovered_nodes"]:
                        self.ipop_interface_details[interface_name]["discovered_nodes"].remove(localuid)

                    log = "recv advertisement: {0}".format(msg["src_uid"])
                    self.registerCBT('Logger', 'info', log)

                    # handle forward packet
                elif msg_type == "forward":
                    dst_uid = msg["dst_uid"]
                    if dst_uid != self.ipop_interface_details[interface_name]["ipop_state"]["_uid"]:
                        self.forward_msg("exact", msg["dst_uid"], msg, interface_name)
                    else:
                        msg["interface_name"] = interface_name
                        if "datagram" in msg.keys():
                            data = msg.pop("datagram")
                            msg["dataframe"] = data
                            self.registerCBT('TincanInterface', 'DO_INSERT_DATA_PACKET', msg)

                # handle find chord
                elif msg_type == "find_chord":
                    if self.forward_msg("closest", msg["dst_uid"], msg, interface_name):
                        # Check whether the current node UID is bigger than the Chord UID
                        if msg["src_uid"] > self.ipop_interface_details[interface_name]["ipop_state"]["_uid"]:
                            self.add_outbound_link("chord", msg["src_uid"], interface_name)
                        else:
                            # forward found_chord message
                            new_msg = {
                                "msg_type": "found_chord",
                                "src_uid": self.ipop_interface_details[interface_name]["ipop_state"]["_uid"],
                                "dst_uid": msg["src_uid"],
                                "log_uid": msg["log_uid"]
                            }

                            self.forward_msg("exact", msg["src_uid"], new_msg, interface_name)

                # handle found chord
                elif msg_type == "found_chord":

                    if self.forward_msg("exact", msg["dst_uid"], msg, interface_name):
                        if msg["src_uid"] > self.ipop_interface_details[interface_name]["ipop_state"]["_uid"]:
                            self.add_outbound_link("chord", msg["src_uid"], interface_name)

                elif msg_type == "add_on_demand":
                    self.add_outbound_link("on_demand", msg["uid"], msg["interface_name"])

                else:
                    log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                        .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
                    self.registerCBT('Logger', 'warning', log)

        elif cbt.action == "get_visualizer_data":
            for interface_name in self.ipop_interface_details.keys():
                interface_details = self.ipop_interface_details[interface_name]
                local_uid = interface_details["ipop_state"]["_uid"]
                local_ip = interface_details["ipop_state"]["_ip4"]
                unmanaged_node_list, successors, chords, on_demands = [], [], [], []

                # Iterate over the IP-UID Table to retrieve Unmanaged node IP list
                for ip, uid in list(interface_details["ip_uid_table"].items()):
                    # check whether the IP is that of the local node
                    if ip != local_ip and uid == local_uid:
                        unmanaged_node_list.append(ip)

                for successor in list(interface_details["successor"].keys()):
                    if "status" in interface_details["successor"][successor].keys():
                        if interface_details["successor"][successor]["status"] == "online":
                            successors.append(successor)

                for chord in list(interface_details["chord"].keys()):
                    if "status" in interface_details["chord"][chord].keys():
                        if interface_details["chord"][chord]["status"] == "online":
                            chords.append(chord)

                for ondemand in list(interface_details["on_demand"].keys()):
                    if "status" in interface_details["on_demand"][ondemand].keys():
                        if interface_details["on_demand"][ondemand]["status"] == "online":
                            on_demands.append(ondemand)
                # Check if GEO IP exists else invoke the function to retrieve the details from Public Stun server
                if interface_details["GeoIP"] in ["", None]:
                    geoip = self.getGeoIP()
                    interface_details["GeoIP"] = geoip
                else:
                    geoip = interface_details["GeoIP"]

                # Message for Overlay visualizer
                new_msg = {
                    "interface_name": interface_name,
                    "uid": local_uid,
                    "ip4": local_ip,
                    "GeoIP": geoip,
                    "mac": interface_details["mac"],
                    "state": interface_details["p2p_state"],
                    "macuidmapping": interface_details["uid_mac_table"],
                    "unmanagednodelist": unmanaged_node_list,
                    "links": {
                        "successor": successors,
                        "chord": chords,
                        "on_demand": on_demands
                    }
                }
                self.registerCBT("OverlayVisualizer", "topology_details", new_msg)

        # handle and forward tincan data packets
        elif cbt.action == "TINCAN_PACKET":
                reqdata = cbt.data
                interface_name = reqdata["interface_name"]
                data = reqdata["dataframe"]
                interface_details = self.ipop_interface_details[interface_name]
                m_type = reqdata["m_type"]
                # ignore packets when not connected to the overlay
                if interface_details["p2p_state"] != "connected":
                    return

                if m_type == "ARP":
                    maclen = int(data[36:38], 16)
                    iplen = int(data[38:40], 16)
                    srcmacindex = 44 + 2 * maclen
                    srcmac = data[44:srcmacindex]
                    srcipindex = srcmacindex + 2 * iplen
                    # srcip = '.'.join(str(int(i, 16)) for i in [data[srcmacindex:srcipindex][i:i + 2] for i in range(0, 8, 2)])
                    destmacindex = srcipindex + 2 * maclen
                    destmac = data[srcipindex:destmacindex]
                    destipindex = destmacindex + 2 * iplen
                    dst_ip = '.'.join(
                        str(int(i, 16)) for i in [data[destmacindex:destipindex][i:i + 2] for i in range(0, 8, 2)])
                else:
                    # src_ip = '.'.join(str(int(i, 16)) for i in [data[52:60][i:i + 2] for i in range(0, 8, 2)])
                    dst_ip = '.'.join(str(int(i, 16)) for i in [data[60:68][i:i + 2] for i in range(0, 8, 2)])
                    destmac, srcmac = data[0:12], data[12:24]

                ip4_uid_table = interface_details["ip_uid_table"]
                if dst_ip in list(ip4_uid_table.keys()):
                    dst_uid = ip4_uid_table[dst_ip]
                elif destmac in interface_details["mac_uid_table"].keys():
                    dst_uid = interface_details["mac_uid_table"][destmac]
                elif dst_ip.split(".")[0] >= "224" or destmac[0:6] == "01005E":
                    self.registerCBT("IPMulticast", "datapacket",
                                     {"dataframe": data, "interface_name": interface_name, "type": "local"})
                    return
                elif destmac == "FFFFFFFFFFFF":
                    datapacket = {
                        "dataframe": data,
                        "interface_name": interface_name,

                    }

                    if reqdata.get("type") == "remote":
                        datapacket["type"] = "remote"
                    else:
                        datapacket["type"] = "local"

                    self.registerCBT("BroadCastForwarder", "BroadcastPkt", datapacket)
                    return
                else:
                    log = "recv illegal tincan_packet: src={0} dst={1}".format(srcmac, destmac)
                    self.registerCBT('Logger', 'info', log)
                    return

                # Message routing to one of the local node attached to this UID
                if dst_uid == interface_details["ipop_state"]["_uid"]:
                    network_inject_message = {
                        "dataframe": data,
                        "interface_name": interface_name
                    }
                    self.registerCBT("TincanInterface", "DO_INSERT_DATA_PACKET", network_inject_message)
                    return

                # send forwarded message
                new_msg = {
                    "msg_type": "forward",
                    "src_uid": interface_details["ipop_state"]["_uid"],
                    "dst_uid": dst_uid,
                    "datagram": data
                }
                if dst_uid not in list(interface_details["peer_uid_sendmsgcount"].keys()):
                    interface_details["peer_uid_sendmsgcount"][dst_uid] = {"count": 1}
                else:
                    interface_details["peer_uid_sendmsgcount"][dst_uid]["count"] += 1

                    if interface_details["peer_uid_sendmsgcount"][dst_uid]["count"] > self.CMConfig["OndemandThreshold"]:
                        if dst_uid in interface_details["online_peer_uid"]:
                            interface_details["peer_uid_sendmsgcount"][dst_uid] = {"count": 0}
                        elif "conn_init_time" not in list(interface_details["peer_uid_sendmsgcount"][dst_uid].keys()):
                            interface_details["peer_uid_sendmsgcount"][dst_uid]["conn_init_time"] = time.time()
                            # add on-demand link
                            self.add_outbound_link("on_demand", dst_uid, interface_name)
                        elif time.time() - interface_details["peer_uid_sendmsgcount"][dst_uid]["conn_init_time"] \
                                > self.CMConfig["OndemandThreshold"] and dst_uid not in interface_details["online_peer_uid"]:
                            interface_details["peer_uid_sendmsgcount"][dst_uid]["conn_init_time"] = time.time()
                            # add on-demand link
                            self.add_outbound_link("on_demand", dst_uid, interface_name)

                self.forward_msg("exact", dst_uid, new_msg, interface_name)

                log = "sent tincan_packet (exact): {0}. Message: {1}".format(dst_uid, data)
                self.registerCBT('Logger', 'info', log)

        else:
            log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                    .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
            self.registerCBT('Logger', 'warning', log)

############################################################################
            # packet forwarding policy                                                 #
############################################################################

    # closer function
    # tests if uid is successively closer to uid_B than uid_A
    def closer(self, uid_A, uid, uid_B):
        if (uid_A < uid_B) and ((uid_A < uid) and (uid <= uid_B)):
            return True  # 0---A===B---N
        elif (uid_A > uid_B) and ((uid_A < uid) or (uid <= uid_B)):
            return True  # 0===B---A===N
        return False

    # forward packet
    #   forward a packet across ICC
    #   - fwd_type = {
    #       exact   = intended specifically to the destination node,
    #       closest = intended to the node closest to the designated node
    #     }
    #   - dst_uid  = UID of the destination or designated node
    #   - msg      = message in transit
    #   returns true if this packet is intended for the calling node

    def forward_msg(self, fwd_type, dst_uid, msg, interface_name):
        # find peer that is successively closest to and less-than-or-equal-to the designated UID
        interface_details = self.ipop_interface_details[interface_name]
        uid = interface_details["ipop_state"]["_uid"]
        nxt_uid = uid
        online_peer_list = list(interface_details["successor"].keys())+list(interface_details["chord"].keys()) +\
                           list(interface_details["on_demand"].keys())
        for peer in sorted(online_peer_list):
            if self.linked(peer, interface_name):
                if peer == dst_uid:
                    nxt_uid = peer
                    break
                if self.closer(uid, peer, dst_uid):
                    nxt_uid = peer

                    # packet is intended specifically to the destination node
        if fwd_type == "exact":
            # this is the destination uid
            if dst_uid == uid:  # if self.uid == dst_uid:
                return True

                # this is the closest node but not the destination; drop packet
            elif nxt_uid == uid:  # elif self.uid == nxt_uid:
                # check if atleast one online peer exists
                if len(online_peer_list) > 0:
                    nxt_uid = max(online_peer_list)
                else:
                    return False

        # packet is intended to the node closest to the designated node
        elif fwd_type == "closest":
            if nxt_uid == uid:  # if self.uid == nxt_uid:
                return True

                # there is a closer node; forward packet to the next node
        self.registerCBT("ConnectionManager", "SEND_ICC_MSG",
                         {"dst_uid": nxt_uid, "msg": msg, "interface_name": interface_name})
        return False

    def linked(self, uid, interface_name):
        if uid in self.ipop_interface_details[interface_name]["successor"].keys():
            if "status" in self.ipop_interface_details[interface_name]["successor"][uid].keys():
                if self.ipop_interface_details[interface_name]["successor"][uid]["status"] == "online":
                    return True
        if uid in self.ipop_interface_details[interface_name]["chord"].keys():
            if "status" in self.ipop_interface_details[interface_name]["chord"][uid].keys():
                if self.ipop_interface_details[interface_name]["chord"][uid]["status"] == "online":
                    return True
        if uid in self.ipop_interface_details[interface_name]["on_demand"].keys():
            if "status" in self.ipop_interface_details[interface_name]["on_demand"][uid].keys():
                if self.ipop_interface_details[interface_name]["on_demand"][uid]["status"] == "online":
                    return True
        return False
############################################################################
    # manage topology #
############################################################################

    def manage_topology(self, interface_name):
        log = "Inside Topology Manager "
        self.registerCBT('Logger', 'debug', log)
        interface_details = self.ipop_interface_details[interface_name]

        online_peer_list = list(interface_details["successor"].keys()) + list(interface_details["chord"].keys()) + \
                           list(interface_details["on_demand"].keys())
        # discover nodes (from XMPP)
        if interface_details["p2p_state"] == "started":
            if not interface_details["ipop_state"]:
                self.registerCBT('Logger', 'info', interface_name + " p2p state: started")
                return
            else:
                interface_details["p2p_state"] = "searching"
                log = "identified local state: {0}".format(interface_details["ipop_state"]["_uid"])
                self.registerCBT('Logger', 'info', log)

        if interface_details["p2p_state"] == "searching":
            if not interface_details["discovered_nodes"]:
                # Get Peer Nodes from the XMPP server
                self.registerCBT('Logger', 'info', interface_name + " p2p state: searching")
                return
            else:
                interface_details["p2p_state"] = "connecting"

        # connecting to the peer-to-peer network
        if interface_details["p2p_state"] == "connecting":
            self.registerCBT('Logger', 'debug', "discovered nodes: {0}".format(interface_details["discovered_nodes"]))
            self.registerCBT('Logger', 'info', interface_name + " p2p state: connecting")
            self.add_successors(interface_name)

            # wait until atleast one successor, chord or on-demand links are created
            for peer in sorted(online_peer_list):
                if self.linked(peer, interface_name):
                    interface_details["p2p_state"] = "connected"
                    self.registerCBT('Logger', 'info', interface_name + " p2p state: CONNECTED")
                    return

        # connecting or connected to the IPOP peer-to-peer network; manage local topology
        if interface_details["p2p_state"] == "connected":
            # manage successors
            self.add_successors(interface_name)
            self.remove_successors(interface_name)
            # manage chords
            self.find_chords(interface_name)

            for peer in sorted(online_peer_list):
                if self.linked(peer, interface_name):
                    interface_details["p2p_state"] = "connected"
                    self.registerCBT('Logger', 'info', interface_name + " p2p state: CONNECTED")
                    return
            interface_details["p2p_state"] = "connecting"
            self.registerCBT('Logger', 'info', interface_name + " p2p state: DISCONNECTED")

    def timer_method(self):
        try:
            for interface_name in self.ipop_interface_details.keys():
                # self.registerCBT("Logger","debug","BTM Table::"+str(self.ipop_interface_details[interface_name]))
                self.manage_topology(interface_name)
                for linktype in ["successor", "chord", "on_demand"]:
                    for uid in self.ipop_interface_details[interface_name][linktype].keys():
                        message = {
                                    "interface_name": interface_name,
                                    "MAC": self.ipop_interface_details[interface_name][linktype][uid]["mac"],
                                    "con_type": linktype,
                                    "uid": uid
                        }
                        self.registerCBT('TincanInterface', 'DO_GET_STATE', message)

                # periodically call policy for link removal
                self.clean_chord(interface_name)
                # self.clean_on_demand(interface_name)  TO DO

        except Exception as err:
            self.registerCBT('Logger', 'error', "Exception in BTM timer:" + str(err))
