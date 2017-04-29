#!/usr/bin/env python
from controller.framework.ControllerModule import ControllerModule
import time
import json


class ConnectionManager(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(ConnectionManager, self).__init__(CFxHandle, paramDict, ModuleName)
        self.CMConfig = paramDict
        self.connection_details = {}
        # Query UID and Tap Interface from VirtualNetworkInitializer
        tincanparams = self.CFxHandle.queryParam("VirtualNetworkInitializer", "Vnets")
        for k in range(len(tincanparams)):
            interface_name = tincanparams[k]["TapName"]
            self.connection_details[interface_name] = {}
            self.connection_details[interface_name]["xmpp_client_code"] = tincanparams[k]["XMPPModuleName"]
            self.connection_details[interface_name]["uid"] = tincanparams[k]["uid"]
            self.connection_details[interface_name]["peers"] = {}
            self.connection_details[interface_name]["online_peer_uid"] = []
        # Member data to hold value for connection retries (value entered in config file)
        self.maxretries = self.CMConfig["MaxConnRetry"]

    def initialize(self):
        # Iterate across Table to send Local Get State request to Tincan
        for interface_name in self.connection_details.keys():
            msg = {"interface_name": interface_name, "MAC": ""}
            self.registerCBT('TincanInterface', 'DO_GET_STATE', msg)
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    # Send Message to XMPP server
    def send_msg_srv(self, msg_type, uid, msg, interface_name):
        cbtdata = {"method": msg_type, "overlay_id": 0, "uid": uid, "data": msg, "interface_name": interface_name}
        self.registerCBT(self.connection_details[interface_name]["xmpp_client_code"], 'DO_SEND_MSG', cbtdata)

    # send message (through ICC)
    #   - uid = UID of the destination peer (a tincan link must exist)
    #   - msg = message
    def send_msg_icc(self, uid, msg, interface_name):
        # Check whether the UID exits in Peer Table
        if uid in self.connection_details[interface_name]["peers"].keys():
            cbtdata = {
                    "src_uid": self.connection_details[interface_name]["ipop_state"]["_uid"],
                    "dst_uid": uid,
                    "dst_mac": self.connection_details[interface_name]["peers"][uid]["mac"],
                    "msg": msg,
                    "interface_name": interface_name
            }
            self.registerCBT('TincanInterface', 'DO_SEND_ICC_MSG', cbtdata)

############################################################################
# connection functions #
############################################################################

    # Request CAS Details from Peer UID for particular conn type(Successor, Chord, On-Demand)
    def request_connection(self, con_type, uid, interface_name):
        conn_details = self.connection_details[interface_name]
        '''
        if uid < conn_details["ipop_state"]["_uid"]:
            self.registerCBT('Logger', 'info',"Dropping connection request to Node with SmallerUID. {0}".format(uid))
            return
        '''
        self.registerCBT('Logger', 'debug', "Peer Table::" + str(conn_details["peers"]))
        # Set the initial connection Time To Live (time within which its status has to change Online)
        ttl = time.time() + self.CMConfig["InitialLinkTTL"]

        # Check whether the request is for a new connection
        if uid not in conn_details["peers"].keys():
            # add peer to peers list
            conn_details["peers"][uid] = {
                    "uid": uid,
                    "ttl": ttl,
                    "con_status": "sent_con_req",
                    "mac": ""
            }
        # check whether the connection request is already in progress but not in
        # connected state then allow connection creation to proceed
        elif conn_details["peers"][uid]["con_status"] not in ["online", "offline"]:
            conn_details["peers"][uid]["ttl"] = ttl
        else:
            return
        # Connection Request Details for Peer
        msg = {
            "con_type": con_type,
            "peer_uid": uid,
            "interface_name": interface_name,
            "ip4": conn_details["ipop_state"]["_ip4"],
            "fpr": conn_details["ipop_state"]["_fpr"],
            "mac": conn_details["mac"],
            "ttl": ttl
        }
        # Send the message via XMPP server to Peer node
        self.send_msg_srv("con_req", uid, json.dumps(msg), interface_name)
        self.registerCBT('Logger', 'info', "Requested CAS details for {1} conn type :{0}".format(con_type, uid))

    # Remove peer connection specified by input UID
    def remove_connection(self, uid, interface_name):
        peer_details = self.connection_details[interface_name]["peers"]
        # Check whether the request for an existing connection else drop
        if uid in peer_details.keys():
            # Check whether the connection has been established
            if peer_details[uid]["con_status"] in ["online", "offline"]:
                if "mac" in list(peer_details[uid].keys()):
                    mac = peer_details[uid]["mac"]
                    if mac is not None and mac != "":
                        msg = {"interface_name": interface_name, "uid": uid, "MAC": mac}
                        self.registerCBT('TincanInterface', 'DO_TRIM_LINK', msg)
            peer_mac = self.connection_details[interface_name]["peers"][uid]["mac"]
            # Send message to BTM to remove the UID from its Table
            self.registerCBT("BaseTopologyManager", "UpdateConnectionDetails", {"uid": uid,
                                                                                "interface_name": interface_name,
                                                                                "msg_type": "remove_peer",
                                                                                "mac": peer_mac})
            del peer_details[uid]
            self.registerCBT('Logger', 'info', "Removed Connection to Peer UID: {0}".format(uid))

    # Update connection details for e.g. Time To Live, Status, Stats, MAC details
    def update_connection(self, data):
        uid = data["uid"]
        interface_name = data["interface_name"]
        if uid in self.connection_details[interface_name]["peers"].keys():
            ttl = self.connection_details[interface_name]["peers"][uid]["ttl"]
            # Check whether the connection is online if yes extend its Time To Live
            if "online" == data["status"]:
                ttl = time.time() + self.CMConfig["LinkPulse"]
                # If the connection has just turned Online added into the Online Peer List
                if uid not in self.connection_details[interface_name]["online_peer_uid"]:
                    self.connection_details[interface_name]["online_peer_uid"].append(uid)
            # Connection has been removed from Tincan clear Connection Manager Table
            elif "unknown" == data["status"]:
                del self.connection_details[interface_name]["peers"][uid]
                if uid in self.connection_details[interface_name]["online_peer_uid"]:
                    self.connection_details[interface_name]["online_peer_uid"].remove(uid)
                return
            else:
                if uid in self.connection_details[interface_name]["online_peer_uid"]:
                    self.connection_details[interface_name]["online_peer_uid"].remove(uid)
            self.connection_details[interface_name]["peers"][uid]["ttl"] = ttl
            self.connection_details[interface_name]["peers"][uid]["stats"] = data["stats"]
            self.connection_details[interface_name]["peers"][uid]["status"] = data["status"]
            self.connection_details[interface_name]["peers"][uid]["con_status"] = data["status"]
            self.connection_details[interface_name]["peers"][uid]["mac"] = data["mac"]

    #  remove peers with expired time-to-live attributes
    def clean_connection(self, interface_name):
        # time-to-live attribute indicative of an offline link
        links = self.connection_details[interface_name]
        # for uid in list(self.ipop_interface_details[interface_name]["peers"].keys()):
        for uid in links["peers"].keys():
            # Check if there exists a link
            # if self.linked(uid,interface_name):
            # check whether the time to link has expired
            if time.time() > links["peers"][uid]["ttl"]:
                log = "Time to Live expired going to remove peer: {0}".format(uid)
                self.registerCBT('Logger', 'info', log)
                self.remove_connection(uid, interface_name)

    # Get CAS details from Tincan
    def respond_connection(self, con_type, uid, data, interface_name):
            peer = self.connection_details[interface_name]["peers"]
            # Get CAS Response Message to Peer
            response_msg = {
                "con_type": con_type,
                "uid": uid,
                "interface_name": interface_name,
                "fpr": self.connection_details[interface_name]["ipop_state"]["_fpr"],
                "cas": data["cas"],
                "ip4": self.connection_details[interface_name]["ipop_state"]["_ip4"],
                "mac": self.connection_details[interface_name]["mac"],
                "peer_mac": data["peer_mac"]
            }

            # If CAS is requested for Peer which is already present in the Table
            if uid in peer.keys():
                log_msg = "Received CAS from Tincan for peer {0} in list.".format(uid)
                self.registerCBT('Logger', 'info', log_msg)
                # Setting Time To Live for the connection
                ttl = time.time() + self.CMConfig["InitialLinkTTL"]
                # if node has received con_req, re-respond (in case it was lost)
                if peer[uid]["con_status"] == "recv_con_req":
                    log_msg = "AIL: Resending respond_connection to {0}".format(uid)
                    self.registerCBT('Logger', 'info', log_msg)
                    response_msg["ttl"] = ttl
                    self.send_msg_srv("con_ack", uid, json.dumps(response_msg), interface_name)
                    # else if node has sent con_request concurrently
                elif peer[uid]["con_status"] == "sent_con_req":
                    # peer with Bigger UID sends a response
                    # if (self.connection_details[interface_name]["ipop_state"]["_uid"] > uid):
                    self.registerCBT('Logger', 'info', "Sending CAS details to peer UID:{0}".format(uid))
                    peer[uid] = {
                            "uid": uid,
                            "ttl": ttl,
                            "con_status": "conc_sent_response",
                            "mac": data["peer_mac"]
                    }
                    response_msg["ttl"] = ttl
                    self.send_msg_srv("con_ack", uid, json.dumps(response_msg), interface_name)
                    '''
                    else:
                        log_msg = "AIL: SmallerUID ignores from {0}".format(uid)
                        self.registerCBT('Logger', 'info', log_msg)
                        peer[uid] = {
                            "uid": uid,
                            "ttl": ttl,
                            "con_status": "conc_no_response",
                            "mac": data["peer_mac"]
                        }
                        return
                    '''
                elif peer[uid]["con_status"] == "offline":
                    # If the CAS has been requested for a connection in progress but
                    if "connretrycount" not in peer[uid].keys():
                        peer[uid]["connretrycount"] = 1
                        response_msg["ttl"] = ttl
                        self.send_msg_srv("con_ack", uid, json.dumps(response_msg), interface_name)
                    else:
                        # Check whether the connection retry has exceed the max count
                        if peer[uid]["connretrycount"] < self.maxretries:
                            peer[uid]["connretrycount"] += 1
                            # Updating Connection Manager Table
                            peer[uid] = {
                                "uid": uid,
                                "ttl": ttl,
                                "con_status": "conc_sent_response",
                                "mac": data["peer_mac"]
                            }
                            self.registerCBT('Logger', 'info', "Sending CAS details to peer UID:{0}".format(uid))
                            response_msg["ttl"] = ttl
                            self.send_msg_srv("con_ack", uid, json.dumps(response_msg), interface_name)
                        else:
                            peer[uid]["connretrycount"] = 0
                            log_msg = "Giving up after max conn retries, removing peer {0}".format(uid)
                            self.registerCBT('Logger', 'warning', log_msg)
                            # Remove the connection as retry has exceeded
                            self.remove_connection(uid, interface_name)
                            # Send CAS details for fresh connection
                            # self.send_msg_srv("con_ack", uid, json.dumps(response_msg), interface_name)
                    # if node was in any other state replied or ignored a concurrent
                    # send request [conc_no_response, conc_sent_response]
                    # or if status is online or offline, remove link and wait to try again
                else:
                    if peer[uid]["con_status"] in ["conc_sent_response", "conc_no_response"]:
                        self.registerCBT('Logger', 'info', "Giving up, remove peer {0}".format(uid))
                        self.remove_connection(uid, interface_name)
            else:
                # add peer to peers list and set status as having received and
                # responded to con_req
                log_msg = "Received CAS from Tincan for peer {0} in list.".format(uid)
                self.registerCBT('Logger', 'info', log_msg)
                # if self.connection_details[interface_name]["ipop_state"]["_uid"] > uid:
                ttl = time.time() + self.CMConfig["InitialLinkTTL"]
                peer[uid] = {
                        "uid": uid,
                        "ttl": ttl,
                        "con_status": "recv_con_req",
                        "mac": data["peer_mac"]
                }
                # connection response
                response_msg["ttl"] = ttl
                self.send_msg_srv("con_ack", uid, json.dumps(response_msg), interface_name)

    # Create connection via Tincan
    def create_connection(self, uid, interface_name, msg):
        con_type = msg["data"]["con_type"]
        peer_mac = msg["data"]["mac"]

        # Create an entry in Conn Manager Table for Peer if does not exists
        if uid not in self.connection_details[interface_name]["peers"].keys():
            self.connection_details[interface_name]["peers"][uid] = {}
        # Update Time To Live for the connection
        self.connection_details[interface_name]["peers"][uid]["ttl"] = time.time() + self.CMConfig[
            "InitialLinkTTL"]
        self.connection_details[interface_name]["peers"][uid]["mac"] = peer_mac
        self.registerCBT('Logger', 'debug', "Received CAS from Peer ({0}): {1}".format(con_type, uid))
        # Send the Create Connection request to Tincan Interface
        self.registerCBT('TincanInterface', 'DO_CREATE_LINK', msg)
        # Update BTM Table for the new connection
        self.registerCBT("BaseTopologyManager", "UpdateConnectionDetails", {"uid": uid,
                                                                            "interface_name": interface_name,
                                                                            "msg_type": "add_peer",
                                                                            "mac": peer_mac,
                                                                            "con_type": con_type,
                                                                            "status": "offline"})

    # Advertise all Online Peer to each node in the network
    def advertise(self, interface_name):
        # create list of linked peers
        peer_list = self.connection_details[interface_name]["online_peer_uid"]
        new_msg = {
            "msg_type": "advertise",
            "src_uid": self.connection_details[interface_name]["ipop_state"]["_uid"],
            "peer_list": peer_list
        }
        # send peer list advertisement to all peers
        for peer in peer_list:
            self.send_msg_icc(peer, new_msg, interface_name)

    def processCBT(self, cbt):
        if cbt.action == "REMOVE_CONNECTION":
            self.remove_connection(cbt.data.get("uid"), cbt.data.get("interface_name"))
        elif cbt.action == "REQUEST_CONNECTION":
            self.request_connection(cbt.data.get("con_type"), cbt.data.get("uid"), cbt.data.get("interface_name"))
        elif cbt.action == "CREATE_LISTENER":
            msg = cbt.data
            msg["data"] = json.loads(msg["data"])
            uid = msg["uid"]
            # interface_name = msg["interface_name"]
            log = "recv con_req ({0}): {1}".format(msg["data"]["con_type"], uid)
            self.registerCBT('Logger', 'debug', log)
            # if uid < self.connection_details[interface_name]["ipop_state"]["_uid"]:
            self.registerCBT('TincanInterface', 'DO_GET_CAS', msg)
        elif cbt.action == "CREATE_CONNECTION":
            msg = cbt.data
            interface_name = msg.get("interface_name")
            msg["data"] = json.loads(msg["data"])
            uid = msg["uid"]
            self.create_connection(uid, interface_name, msg)
        elif cbt.action == "UPDATE_CONNECTION":
            self.update_connection(cbt.data)
        elif cbt.action == "GET_CAS_DETAILS":
            msg = cbt.data
            interface_name = msg["interface_name"]
            self.registerCBT("BaseTopologyManager", "UpdateConnectionDetails", {"uid": msg["uid"],
            "interface_name": interface_name, "msg_type": "add_peer", "mac": msg["data"]["peer_mac"],
            "con_type": msg["data"]["con_type"], "status": "recv_con_req"})
            self.respond_connection(msg["data"]["con_type"], msg["uid"], msg["data"], interface_name)
        elif cbt.action == "SEND_ICC_MSG":
            msg = cbt.data
            self.send_msg_icc(msg.get("dst_uid"), msg.get("msg"), msg.get("interface_name"))
        elif cbt.action == "GET_NODE_MAC_ADDRESS":
            interface_name = cbt.data.get("interface_name")
            if "mac" in self.connection_details[interface_name].keys():
                self.registerCBT(cbt.initiator, "NODE_MAC_ADDRESS",
                                 {"interface_name": interface_name, "localmac": self.connection_details[interface_name]["mac"]})
            else:
                self.registerCBT(cbt.initiator, "NODE_MAC_ADDRESS",
                                 {"interface_name": interface_name, "localmac": ""})
        elif cbt.action == "TINCAN_CONTROL":
            msg = cbt.data
            msg_type = msg.get("type", None)
            interface_name = msg["interface_name"]
            interface_details = self.connection_details[interface_name]
            # update local state
            if msg_type == "local_state":
                interface_details["ipop_state"] = msg
                interface_details["mac"] = msg["mac"]
                self.registerCBT("Logger", "info", "Local Node Info UID:\
                {0} MAC:{1} IP4: {2}".format(msg["_uid"], msg["mac"], msg["_ip4"]))
            elif msg_type == "get_online_peerlist":
                interface_name = cbt.data["interface_name"]
                cbtdt = {'peerlist': self.connection_details[interface_name]["online_peer_uid"],
                         'uid': interface_details["ipop_state"]["_uid"],
                         'mac': interface_details["mac"],
                         'interface_name': interface_name
                         }
                # Send the Online PeerList to the Initiator of CBT
                self.registerCBT(cbt.initiator, 'ONLINE_PEERLIST', cbtdt)
            else:
                log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                    .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
                self.registerCBT('Logger', 'warning', log)
        else:
            log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                    .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
            self.registerCBT('Logger', 'warning', log)

    def timer_method(self):
        try:
            for interface_name in self.connection_details.keys():
                # self.registerCBT("Logger","debug","Peer Nodes:: {0}".
                # format(self.connection_details[interface_name]["peers"]))
                self.clean_connection(interface_name)
                self.advertise(interface_name)
        except Exception as err:
            self.registerCBT('Logger', 'error', "Exception caught in Connection Manager timer thread.\
                             Error: {0}".format(str(err)))

    def terminate(self):
        pass
