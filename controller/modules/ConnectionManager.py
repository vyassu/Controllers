from controller.framework.ControllerModule import ControllerModule
import time,json,math

class ConnectionManager(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(ConnectionManager, self).__init__(CFxHandle, paramDict, ModuleName)
        self.CMConfig = paramDict
        self.connection_details = {}
        tincanparams = self.CFxHandle.queryParam("VirtualNetworkInitializer", "Vnets")
        for k in range(len(tincanparams)):
            interface_name = tincanparams[k]["TapName"]
            self.connection_details[interface_name]          = {}
            self.connection_details[interface_name]["xmpp_client_code"] = tincanparams[k]["XMPPModuleName"]
            self.connection_details[interface_name]["uid"]   = tincanparams[k]["uid"]
            self.connection_details[interface_name]["peers"] = {}
            self.connection_details[interface_name]["online_peer_uid"] = []
        self.maxretries = self.CMConfig["MaxConnRetry"]
        tincanparams = None

    def initialize(self):
        # Get Peer Nodes from XMPP server
        for interface_name in self.connection_details.keys():
            msg = {"interface_name": interface_name, "MAC": ""}
            self.registerCBT('TincanInterface', 'DO_GET_STATE', msg)
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def send_msg_srv(self, msg_type, uid, msg,interface_name):
        cbtdata = {"method": msg_type, "overlay_id": 0, "uid": uid, "data": msg,"interface_name":interface_name}
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
            self.registerCBT("Logger", "debug", "ICC Message overlay" + str(cbtdata))
            self.registerCBT('TincanInterface', 'DO_SEND_ICC_MSG', cbtdata)

############################################################################
                    # connection functions #
############################################################################

    # request connection
    def request_connection(self, con_type, uid, interface_name):
        conn_details = self.connection_details[interface_name]
        # add peer to link type

        if uid < conn_details["ipop_state"]["_uid"]:
            self.registerCBT('Logger', 'info',"Dropping connection request to Node with SmallerUID. {0}".format(uid))
            return
        self.registerCBT('Logger', 'debug', "Peer Table::" + str(conn_details["peers"]))

        # Connection Request Message
        ttl = time.time() + self.CMConfig["InitialLinkTTL"]
        msg = {
                "con_type": con_type,
                "peer_uid": uid,
                "interface_name": interface_name,
                "ip4": conn_details["ipop_state"]["_ip4"],
                "fpr": conn_details["ipop_state"]["_fpr"],
                "mac": conn_details["mac"],
                "ttl": ttl
        }

        # peer is not in the peers list
        if uid not in conn_details["peers"].keys():
            # add peer to peers list
            conn_details["peers"][uid] = {
                    "uid": uid,
                    "ttl": ttl,
                    "con_status": "sent_con_req",
                    "mac": ""
            }
        elif conn_details["peers"][uid]["con_status"] not in ["offline", "online"]:
            conn_details["peers"][uid]["ttl"] = ttl

        try:
            self.send_msg_srv("con_req", uid, json.dumps(msg),interface_name)
            log = "sent con_req ({0}): {1}".format(con_type, uid)
            self.registerCBT('Logger', 'debug', log)
        except:
            self.registerCBT('Logger', 'info', "Exception in send_msg_srv con_req")



    def remove_connection(self, uid, interface_name):
        peer_details = self.connection_details[interface_name]["peers"]
        if uid in peer_details.keys():
            if peer_details[uid]["con_status"] in ["online", "offline"]:
                if "mac" in list(peer_details[uid].keys()):
                    mac = peer_details[uid]["mac"]
                    if mac != None and mac != "":
                        msg = {"interface_name": interface_name, "uid": uid, "MAC": mac}
                        self.registerCBT('TincanInterface', 'DO_TRIM_LINK', msg)
                        log = "removed connection: {0}".format(uid)
                        self.registerCBT('Logger', 'info', log)
            peer_mac = self.connection_details[interface_name]["peers"][uid]["mac"]
            self.registerCBT("BaseTopologyManager", "UpdateConnectionDetails",
                         {"uid": uid, "interface_name": interface_name, "msg_type": "remove_peer", "mac": peer_mac})
            del peer_details[uid]

    def update_connection(self, data):
        uid = data["uid"]
        interface_name = data["interface_name"]
        if uid in self.connection_details[interface_name]["peers"].keys():
            ttl = self.connection_details[interface_name]["peers"][uid]["ttl"]
            if "online" == data["status"]:
                ttl = time.time() + self.CMConfig["LinkPulse"]
            self.connection_details[interface_name]["peers"][uid]["ttl"] = ttl
            self.connection_details[interface_name]["peers"][uid]["stats"] = data["stats"]
            self.connection_details[interface_name]["peers"][uid]["status"] = data["status"]
            self.connection_details[interface_name]["peers"][uid]["con_status"] = data["status"]
            self.connection_details[interface_name]["peers"][uid]["mac"] = data["mac"]
    # clean connections

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




    def respond_connection(self, con_type, uid, data, interface_name):
            # recvd con_req and sender is in peers_list - uncommon case
            peer = self.connection_details[interface_name]["peers"]
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


            if (uid in peer.keys()):
                log_msg = "AIL: Recvd con_req for peer in list from {0} status {1}".format(uid, peer[uid][
                    "con_status"])
                self.registerCBT('Logger', 'info', log_msg)
                ttl = time.time() + self.CMConfig["InitialLinkTTL"]
                # if node has received con_req, re-respond (in case it was lost)
                if (peer[uid]["con_status"] == "recv_con_req"):
                    log_msg = "AIL: Resending respond_connection to {0}".format(uid)
                    self.registerCBT('Logger', 'info', log_msg)
                    # self.respond_connection(con_type, uid, fpr, interface_name)
                    response_msg["ttl"] = ttl
                    # self.registerCBT("ConnectionManager", "respond_connection", response_msg)
                    self.send_msg_srv("con_ack", uid, json.dumps(response_msg), interface_name)
                    # else if node has sent con_request concurrently
                elif (peer[uid]["con_status"] == "sent_con_req"):
                    # peer with Bigger UID sends a response
                    if (self.connection_details[interface_name]["ipop_state"]["_uid"] > uid):
                        log_msg = "AIL: LargerUID respond_connection to {0}".format(uid)
                        self.registerCBT('Logger', 'info', log_msg)

                        peer[uid] = {
                            "uid": uid,
                            "ttl": ttl,
                            "con_status": "conc_sent_response",
                            "mac": data["peer_mac"]
                        }
                        # self.respond_connection(con_type, uid, fpr, interface_name)
                        response_msg["ttl"] = ttl
                        #self.registerCBT("ConnectionManager", "respond_connection", response_msg)
                        self.send_msg_srv("con_ack", uid, json.dumps(response_msg), interface_name)
                    # peer with larger UID ignores
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
                elif peer[uid]["con_status"] == "offline":
                    if "connretrycount" not in peer[uid].keys():
                        peer[uid]["connretrycount"] = 0
                        response_msg["ttl"] = ttl
                        #self.registerCBT("ConnectionManager", "respond_connection", response_msg)
                        self.send_msg_srv("con_ack", uid, json.dumps(response_msg), interface_name)
                    else:
                        if peer[uid]["connretrycount"] < self.maxretries:
                            peer[uid]["connretrycount"] += 1
                            peer[uid] = {
                                "uid": uid,
                                "ttl": ttl,
                                "con_status": "conc_sent_response",
                                "mac": data["peer_mac"]
                            }
                            log_msg = "AIL: Resending respond_connection to {0}".format(uid)
                            self.registerCBT('Logger', 'info', log_msg)
                            response_msg["ttl"] = ttl
                            self.send_msg_srv("con_ack", uid, json.dumps(response_msg), interface_name)
                        else:
                            peer[uid]["connretrycount"] = 0
                            log_msg = "AIL: Giving up after max conn retries, remove_connection from {0}".format(
                                uid)
                            self.registerCBT('Logger', 'warning', log_msg)
                            self.remove_connection(uid, interface_name)
                            '''
                            if uid in peer.keys():
                                peer.pop(uid)
                                self.registerCBT("ConnectionManager", "remove_connection", remove_link_msg)
                            '''
                # if node was in any other state:
                # replied or ignored a concurrent send request:
                #    conc_no_response, conc_sent_response
                # or if status is online or offline,
                # remove link and wait to try again
                else:
                    if peer[uid]["con_status"] in ["conc_sent_response", "conc_no_response"]:
                        log_msg = "AIL: Giving up, remove_connection from {0}".format(uid)
                        self.registerCBT('Logger', 'info', log_msg)
                        self.remove_connection(uid, interface_name)
                        '''
                        if uid in self.ipop_interface_details[interface_name]["peers"]:
                            self.ipop_interface_details[interface_name]["peers"].pop(uid)
                            self.registerCBT("ConnectionManager", "remove_connection", remove_link_msg)
                            # recvd con_req and sender is not in peers list - common case
                        '''
            else:
                # add peer to peers list and set status as having received and
                # responded to con_req
                log_msg = "AIL: Recvd con_req for peer not in list {0}".format(uid)
                self.registerCBT('Logger', 'info', log_msg)
                if self.connection_details[interface_name]["ipop_state"]["_uid"] > uid:
                    ttl = time.time() + self.CMConfig["InitialLinkTTL"]
                    peer[uid] = {
                        "uid": uid,
                        "ttl": ttl,
                        "con_status": "recv_con_req",
                        "mac": data["peer_mac"]
                    }
                    # connection response
                    # self.respond_connection(con_type, uid, fpr, interface_name)
                    response_msg["ttl"] = ttl
                    #self.registerCBT("ConnectionManager", "respond_connection", response_msg)
                    self.send_msg_srv("con_ack", uid, json.dumps(response_msg), interface_name)

    def create_connection(self,uid,interface_name,msg):
        con_type = msg["data"]["con_type"]
        peer_mac = msg["data"]["mac"]

        if uid not in self.connection_details[interface_name]["peers"].keys():
            self.connection_details[interface_name]["peers"][uid] = {}
        self.connection_details[interface_name]["peers"][uid]["ttl"] = time.time() + self.CMConfig[
            "InitialLinkTTL"]
        self.connection_details[interface_name]["peers"][uid]["mac"] = peer_mac
        log = "recvd con_ack ({0}): {1}".format(con_type, uid)
        self.registerCBT('Logger', 'debug', log)
        self.registerCBT('TincanInterface', 'DO_CREATE_LINK', msg)
        self.registerCBT("BaseTopologyManager", "UpdateConnectionDetails",
        {"uid": uid, "interface_name": interface_name, "msg_type": "add_peer","mac":peer_mac,"con_type":con_type})


    def advertise(self,interface_name):
        # create list of linked peers
        peer_list = self.connection_details[interface_name]["online_peer_uid"]
        # send peer list advertisement to all peers
        new_msg = {
            "msg_type": "advertise",
            "src_uid": self.connection_details[interface_name]["ipop_state"]["_uid"],
            "peer_list": peer_list
        }

        for peer in peer_list:
            self.send_msg_icc(peer, new_msg,interface_name)

    def processCBT(self,cbt):
        if cbt.action == "remove_connection":
            self.remove_connection(cbt.data.get("uid"),cbt.data.get("interface_name"))
        elif cbt.action == "request_connection":
            self.request_connection(cbt.data.get("con_type"),cbt.data.get("uid"),\
                                    cbt.data.get("interface_name"))
        elif cbt.action == "respond_connection":
            msg = cbt.data
            msg["data"] = json.loads(msg["data"])
            uid = msg["uid"]
            interface_name = msg["interface_name"]
            log = "recv con_req ({0}): {1}".format(msg["data"]["con_type"], uid)
            self.registerCBT('Logger', 'debug', log)
            if uid < self.connection_details[interface_name]["ipop_state"]["_uid"]:
                self.registerCBT('TincanInterface', 'DO_GET_CAS', msg)

        elif cbt.action == "create_connection":
            msg = cbt.data
            interface_name = msg.get("interface_name")
            msg["data"] = json.loads(msg["data"])
            uid = msg["uid"]

            self.create_connection(uid,interface_name,msg)

        elif cbt.action == "update_connection":
            self.update_connection(cbt.data)
        elif cbt.action == "receive_cas_details":
            msg = cbt.data
            interface_name = msg["interface_name"]
            self.registerCBT("BaseTopologyManager", "UpdateConnectionDetails",
                             {"uid": msg["uid"], "interface_name": interface_name, "msg_type": "add_peer", "mac": msg["data"]["peer_mac"],
                              "con_type": msg["data"]["con_type"]})
            self.respond_connection(msg["data"]["con_type"], msg["uid"], msg["data"],interface_name)

        elif cbt.action == "SendICCMessage":
            msg = cbt.data
            self.send_msg_icc(msg.get("dst_uid"),msg.get("msg"),msg.get("interface_name"))

        elif cbt.action == "TINCAN_CONTROL":
            msg = cbt.data
            msg_type = msg.get("type", None)
            interface_name = msg["interface_name"]
            interface_details = self.connection_details[interface_name]
            # update local state
            if msg_type == "local_state":
                interface_details["ipop_state"] = msg
                interface_details["mac"] = msg["mac"]
                # Send MAC Address to ARPManager module
                self.registerCBT("NodeDiscovery", "getlocalmacaddress",
                                 {"interface_name": interface_name, "localmac": msg["mac"]})
                self.registerCBT("Logger", "info", "Local Node Info UID:\
                {0} MAC:{1} IP4: {2}".format(msg["_uid"], msg["mac"],msg["_ip4"]))
            # update peer list
            elif msg_type == "peer_state":
                uid = msg["uid"]
                if uid in interface_details["peers"]:
                    self.registerCBT("Logger", "info", "ConnectionManager"+str(self.connection_details))
                    # preserve ttl and con_status attributes
                    ttl = interface_details["peers"][uid]["ttl"]
                    connretry = 0
                    if "connretrycount" in interface_details["peers"][uid].keys():
                        connretry = interface_details["peers"][uid]["connretrycount"]
                    # update ttl attribute
                    if "online" == msg["status"]:
                        ttl = time.time() + self.CMConfig["LinkPulse"]
                        if uid not in interface_details["online_peer_uid"]:
                            interface_details["online_peer_uid"].append(uid)
                    elif "unknown" == msg["status"]:
                        del interface_details["peers"][uid]
                        if uid in interface_details["online_peer_uid"]:
                            interface_details["online_peer_uid"].remove(msg["uid"])
                        return
                    else:
                        if msg["uid"] in interface_details["online_peer_uid"]:
                            interface_details["online_peer_uid"].remove(uid)

                    # update peer state
                    interface_details["peers"][uid].update(msg)
                    interface_details["peers"][uid]["ttl"] = ttl
                    interface_details["peers"][uid]["con_status"] = msg["status"]
                    interface_details["peers"][uid]["connretrycount"] = connretry

            elif msg_type == "GetOnlinePeerList":
                interface_name = cbt.data["interface_name"]
                cbtdt = {'peerlist': self.connection_details[interface_name]["online_peer_uid"],
                         'uid': interface_details["ipop_state"]["_uid"],
                         'mac': interface_details["mac"],
                         'interface_name': interface_name
                         }

                self.registerCBT('BroadCastForwarder', 'peer_list', cbtdt)

    def timer_method(self):
        #try:
            for interface_name in self.connection_details.keys():
                self.registerCBT("Logger","info","Peer Nodes:: {0}".format(self.connection_details[interface_name]["peers"]))
                '''
                peer_list = list(self.connection_details[interface_name]["peers"].keys())
                for uid in peer_list:
                    message = {
                                "interface_name": interface_name,
                                "MAC": self.connection_details[interface_name]["peers"][uid]["mac"],
                                "uid": uid}
                    self.registerCBT('TincanInterface', 'DO_GET_STATE', message)
                '''
                self.clean_connection(interface_name)
                self.advertise(interface_name)
        #except Exception as err:
            #self.registerCBT('Logger', 'error', "Exception in Connection Manager timer thread.\
                             #Err: {0}".format(str(err)))

    def terminate(self):
        pass


