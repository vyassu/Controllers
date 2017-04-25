from controller.framework.ControllerModule import ControllerModule
import socket,select,json,ast
import controller.framework.ipoplib as ipoplib
from threading import Thread

class TincanInterface(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(TincanInterface, self).__init__(CFxHandle, paramDict, ModuleName)
        self.trans_counter = 0
        self.CONFIG  = paramDict
        if socket.has_ipv6:
            self.sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            self.sock_svr = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            self.sock_svr.bind((self.CONFIG["localhost6"], self.CONFIG["ctrl_recv_port"]))
            self.dest = (self.CONFIG["localhost6"], self.CONFIG["ctrl_send_port"])
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock_svr = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock_svr.bind((self.CONFIG["localhost"],self.CONFIG["ctrl_recv_port"]))
            self.dest = (self.CONFIG["localhost"], self.CONFIG["ctrl_send_port"])

        self.sock.bind(("", 0))
        self.sock_list = [self.sock_svr]

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

        # create a listener thread (listens to tincan notifications
        self.TincanListenerThread = Thread(target=self.__tincan_listener)
        self.TincanListenerThread.setDaemon(True)
        self.TincanListenerThread.start()

    def __tincan_listener(self):
        while True:
            socks, _, _ = select.select(self.sock_list, [], [],
                                        self.CONFIG["SocketReadWaitTime"])

            for sock in socks:
                if sock == self.sock_svr:
                    data,addr = sock.recvfrom(self.CMConfig["buf_size"])
                    self.registerCBT('TincanInterface', 'PROCESS_TINCAN_DATA', data)

    def processCBT(self, cbt):
        if cbt.action == 'DO_CREATE_LINK':
            uid = cbt.data.get('uid')
            msg = cbt.data.get("data")
            log = "Creating conn to : {0}".format(uid)
            self.registerCBT('Logger', 'info', log)

            connection_details = ipoplib.CONCT
            conn_details = connection_details["IPOP"]["Request"]
            conn_details["InterfaceName"]                 = cbt.data.get("interface_name")
            connection_details["IPOP"]["TransactionId"]   = self.trans_counter
            self.trans_counter +=1
            conn_details["PeerInfo"]["VIP4"]      = msg.get('ip4')
            conn_details["PeerInfo"]["UID"]       = uid
            conn_details["PeerInfo"]["MAC"]       = msg.get('mac')
            conn_details["PeerInfo"]["CAS"]       = msg.get('cas')
            conn_details["PeerInfo"]["Fingerprint"] = msg.get('fpr')
            conn_details["PeerInfo"]["con_type"] = msg.get("con_type")
            conn_details["Initiator"] = cbt.initiator
            self.send_msg(json.dumps(connection_details))
            log = "Connection Details : {0}".format(str(conn_details))
            self.registerCBT('Logger', 'debug', log)

        elif cbt.action == 'DO_TRIM_LINK':
            uid = cbt.data.get("uid")
            log = "removing conn to : {0}".format(uid)
            self.registerCBT('Logger', 'info', log)

            remove_node_details = ipoplib.REMOVE
            remove_node_details["IPOP"]["Request"]["InterfaceName"] = cbt.data.get("interface_name")
            remove_node_details["IPOP"]["Request"]["Initiator"] = cbt.initiator
            remove_node_details["IPOP"]["TransactionId"] = self.trans_counter
            self.trans_counter+=1
            #remove_node_details["IPOP"]["Request"]["UID"]           = uid
            remove_node_details["IPOP"]["Request"]["MAC"] = cbt.data.get("MAC")
            self.send_msg(json.dumps(remove_node_details))
            log = "Tincan Request : {0}".format(str(remove_node_details["IPOP"]))
            self.registerCBT('Logger', 'debug', log)

        elif cbt.action == 'DO_GET_STATE':
            get_state_request = ipoplib.LSTATE
            get_state_request["IPOP"]["TransactionId"]              = self.trans_counter
            self.trans_counter+=1
            get_state_request["IPOP"]["Request"]["ProtocolVersion"] = 4
            get_state_request["IPOP"]["Request"]["InterfaceName"] = cbt.data.get("interface_name")
            get_state_request["IPOP"]["Request"]["UID"]           = cbt.data.get("uid")
            get_state_request["IPOP"]["Request"]["MAC"]           = cbt.data.get("MAC")
            get_state_request["IPOP"]["Request"]["con_type"]      = cbt.data.get("con_type",None)
            get_state_request["IPOP"]["Request"]["Initiator"] = cbt.initiator
            self.send_msg(json.dumps(get_state_request))
            log = "Tincan Request : {0}".format(str(get_state_request["IPOP"]))
            self.registerCBT('Logger', 'debug', log)

        elif cbt.action  == 'DO_GET_CAS':
            lcas = ipoplib.LCAS
            data = cbt.data
            uid  = data["uid"]
            lcas["IPOP"]["TransactionId"]                      = self.trans_counter
            self.trans_counter  +=1
            lcas["IPOP"]["Request"]["InterfaceName"]           = data["interface_name"]
            lcas["IPOP"]["Request"]["PeerInfo"]["VIP4"]        = data["data"]["ip4"]
            #lcas["IPOP"]["Request"]["PeerInfo"]["VIP6"]        = data["data"]["ip6"]
            lcas["IPOP"]["Request"]["PeerInfo"]["Fingerprint"] = data["data"]["fpr"]
            lcas["IPOP"]["Request"]["PeerInfo"]["UID"]         = uid
            lcas["IPOP"]["Request"]["PeerInfo"]["MAC"]         = data["data"]["mac"]
            lcas["IPOP"]["Request"]["PeerInfo"]["con_type"]    = data["data"]["con_type"]
            lcas["IPOP"]["Request"]["Initiator"] = cbt.initiator
            log = "Get CAS Request :: {0}".format(str(lcas["IPOP"]))
            self.registerCBT('Logger', 'debug', log)
            self.send_msg(json.dumps(lcas))

        elif cbt.action == 'DO_ECHO':
            ec = ipoplib.ECHO
            ec["IPOP"]["Request"]["InterfaceName"] = cbt.data.get("interface_name")
            ec["IPOP"]["Request"]["TransactionId"] = self.trans_counter
            ec["IPOP"]["Request"]["Initiator"] = cbt.initiator
            self.trans_counter+=1
            self.send_msg(json.dumps(ec))

        elif cbt.action == 'DO_SEND_ICC_MSG':
            icc_message_details = ipoplib.ICC
            icc_message_details["IPOP"]["TransactionId"] = self.trans_counter
            self.trans_counter += 1
            icc_message_details["IPOP"]["Request"]["InterfaceName"]   = cbt.data.get("interface_name")
            icc_message_details["IPOP"]["Request"]["Recipient"]       = cbt.data.get('dst_uid')
            icc_message_details["IPOP"]["Request"]["RecipientMac"]    = cbt.data.get('dst_mac')
            msg  = cbt.data.get("msg")
            icc_message_details["IPOP"]["Request"]["Data"]      = json.dumps(msg)
            icc_message_details["IPOP"]["Request"]["Initiator"] = cbt.initiator
            self.send_msg(json.dumps(icc_message_details))
            log = "ICC Message : {0}".format(str(icc_message_details["IPOP"]))
            self.registerCBT('Logger', 'debug', log)

        elif cbt.action == 'DO_INSERT_DATA_PACKET':
            packet = ipoplib.INSERT_TAP_PACKET
            packet["IPOP"]["TransactionId"] = self.trans_counter
            self.trans_counter +=1
            packet["IPOP"]["Request"]["InterfaceName"]   = cbt.data["interface_name"]
            packet["IPOP"]["Request"]["Data"]      = cbt.data["dataframe"]
            self.send_msg(json.dumps(packet))
            log = "Network Packet Inserted: {0}".format(str(packet["IPOP"]))
            self.registerCBT('Logger', 'info', log)

        elif cbt.action == 'DO_RETRY':
            self.send_msg(json.dumps(cbt.data))

        elif cbt.action == "DO_INSERT_ROUTING_RULES":
            add_routing = ipoplib.ADD_ROUTING
            add_routing["IPOP"]["TransactionId"] = self.trans_counter
            self.trans_counter += 1
            add_routing["IPOP"]["Request"]["InterfaceName"] = cbt.data["interface_name"]
            add_routing["IPOP"]["Request"]["Routes"]        = []
            sourcemac = cbt.data["sourcemac"]
            for mac in cbt.data.get("destmac"):
                if mac != "0"*12 and mac != sourcemac:
                    add_routing["IPOP"]["Request"]["Routes"]= [mac+":"+sourcemac]

                    log = "Routing Rule Inserted: {0}".format(str(add_routing["IPOP"]))
                    self.registerCBT('Logger', 'debug', log)

                    self.send_msg(json.dumps(add_routing))

        elif cbt.action == "DO_REMOVE_ROUTING_RULES":
            remove_routing = ipoplib.DELETE_ROUTING
            remove_routing["IPOP"]["TransactionId"] = self.trans_counter
            self.trans_counter += 1
            remove_routing["IPOP"]["Request"]["InterfaceName"] = cbt.data["interface_name"]
            remove_routing["IPOP"]["Request"]["Routes"]        = [ cbt.data["mac"] ]

            log = "Routing Rule Removed: {0}".format(str(remove_routing["IPOP"]))
            self.registerCBT('Logger', 'debug', log)
            self.send_msg(json.dumps(remove_routing))

        elif cbt.action == "DO_SEND_TINCAN_MSG":
            data = cbt.data
            data["IPOP"]["TransactionId"] = self.trans_counter
            self.trans_counter += 1
            data["IPOP"]["Request"]["Initiator"] = cbt.initiator
            self.send_msg(json.dumps(data))
            log = "Data sent to Tincan: {0}".format(str(data))
            self.registerCBT('Logger', 'debug', log)

        elif cbt.action == "PROCESS_TINCAN_DATA":
            interface_name = ""
            data = cbt.data
            tincan_resp_msg = json.loads(data.decode("utf-8"))["IPOP"]
            req_operation = tincan_resp_msg["Request"]["Command"]

            # Check if tap name exits in the TINCAN Response Message
            if "InterfaceName" in tincan_resp_msg["Request"].keys():
                interface_name = tincan_resp_msg["Request"]["InterfaceName"]

            # Check whether Tincan Message is UpdateRoute or ICC Message
            if "Response" in tincan_resp_msg.keys():

                # check whether the Tican response is Success message
                if tincan_resp_msg["Response"]["Success"] == True:
                    if req_operation == "QueryNodeInfo":
                        resp_msg = json.loads(tincan_resp_msg["Response"]["Message"])
                        resp_target_module = tincan_resp_msg["Request"]["Initiator"]
                        # Check whether the Query is for Self or a remote node
                        if resp_msg["Type"] == "local":
                            msg = {
                                "type": "local_state",
                                "_uid": resp_msg["UID"],
                                "_ip4": resp_msg["VIP4"],
                                # "_ip6": resp_msg["VIP6"],
                                "_fpr": resp_msg["Fingerprint"],
                                "mac": resp_msg["MAC"],
                                "interface_name": interface_name
                            }
                            log = "current state of {0} : {1}".format(resp_msg["UID"], str(msg))
                            self.registerCBT('Logger', 'debug', log)
                            self.registerCBT(resp_target_module, 'TINCAN_CONTROL', msg)
                        else:
                            # Checks whether the link to peer is in Unknown state
                            if resp_msg["Status"] != "unknown":
                                msg = {
                                    "type": "peer_state",
                                    "uid": tincan_resp_msg["Request"]["UID"],
                                    "ip4": resp_msg["VIP4"],
                                    "con_type": tincan_resp_msg["Request"].get("con_type",None),
                                    "fpr": resp_msg["Fingerprint"],
                                    "mac": resp_msg["MAC"],
                                    "status": resp_msg["Status"],
                                    "stats": resp_msg["Stats"],
                                    "interface_name": interface_name
                                }
                            else:
                                msg = {
                                    "type": "peer_state",
                                    "uid": tincan_resp_msg["Request"]["UID"],
                                    "ip4": "",
                                    "con_type": tincan_resp_msg["Request"].get("con_type",None),
                                    "fpr": "",
                                    "mac": "",
                                    "ttl": "",
                                    "rate": "",
                                    "stats": [],
                                    "status": resp_msg["Status"],
                                    "interface_name": interface_name
                                }
                            log = "current state of {0} : {1}".format(tincan_resp_msg["Request"]["UID"], str(msg))
                            self.registerCBT('Logger', 'debug', log)
                            #self.registerCBT('BaseTopologyManager', 'TINCAN_CONTROL', msg)
                            self.registerCBT(resp_target_module, 'TINCAN_CONTROL', msg)

                    elif req_operation == "CreateLinkListener":
                        # Sends the CAS to BaseTopologyManger
                        resp_target_module = tincan_resp_msg["Request"]["Initiator"]
                        log = "recv data from Tincan for operation: {0}".format(tincan_resp_msg["Request"]["Command"])
                        self.registerCBT('Logger', 'info', log)
                        self.registerCBT('Logger', 'debug', "Message: " + str(tincan_resp_msg))
                        msg = {
                            "type": "con_ack",
                            "uid": tincan_resp_msg["Request"]["PeerInfo"]["UID"],
                            "data": {
                                "fpr": tincan_resp_msg["Request"]["PeerInfo"]["Fingerprint"],
                                "cas": tincan_resp_msg['Response']['Message'],
                                "con_type": tincan_resp_msg["Request"]["PeerInfo"]["con_type"],
                                "peer_mac": tincan_resp_msg["Request"]["PeerInfo"]["MAC"]
                            },
                            "interface_name": interface_name
                        }
                        #self.registerCBT('BaseTopologyManager', 'TINCAN_CONTROL', msg)
                        #self.registerCBT(resp_target_module, 'TINCAN_CONTROL', msg)
                        self.registerCBT(resp_target_module, 'receive_cas_details', msg)
                    elif req_operation == "ConnectToPeer":
                        # Response message for Connection Request for a link
                        log = "recv data from Tincan for operation: {0}".format(tincan_resp_msg["Request"]["Command"])
                        self.registerCBT('Logger', 'info', log)
                        self.registerCBT('Logger', 'debug', "Message: " + str(tincan_resp_msg))
                        msg = {
                            "type": "con_resp",
                            "uid": tincan_resp_msg["Request"]["PeerInfo"]["UID"],
                            "data": {
                                "fpr": tincan_resp_msg["Request"]["PeerInfo"]["Fingerprint"],
                                "cas": tincan_resp_msg["Request"]["PeerInfo"]["CAS"],
                                "con_type": tincan_resp_msg["Request"]["PeerInfo"]["con_type"]
                            },
                            "status": "offline",
                            "interface_name": interface_name
                        }
                        return
                    elif req_operation in ["CreateCtrlRespLink","ConfigureLogging","CreateVnet","SetIgnoredNetInterfaces"]:
                        self.registerCBT("Logger","info","recv data from Tincan for operation: {0}. Task status::{1}".format(req_operation,tincan_resp_msg["Response"]))
                        return
                    else:
                        log = '{0}: unrecognized Data {1} received from {2}. Data:::{3}' \
                            .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
                        self.registerCBT('Logger', 'warning', log)
                else:
                    log = 'Tincan Failure:: '.format(cbt.data)
                    self.registerCBT('Logger', 'warning', log)
            else:
                # Request Message to query online peers from BTM
                req_peer_list = {
                    "interface_name": interface_name,
                    "type": "GetOnlinePeerList",
                }

                # Checks whether the message is ICC or UpdateRoute
                if req_operation == "ICC":
                    log = "recv data from Tincan for operation: {0}".format(tincan_resp_msg["Request"]["Command"])
                    self.registerCBT('Logger', 'debug', log)
                    iccmsg = json.loads(tincan_resp_msg["Request"]["Data"])

                    self.registerCBT('ConnectionManager', 'TINCAN_CONTROL', req_peer_list)
                    self.registerCBT('Logger', 'debug', "ICC Message Received ::" + str(iccmsg))

                    # Checks whether ICC message is routing related
                    if "msg" in iccmsg.keys():
                        iccmsg["msg"]["type"] = "remote"
                        iccmsg["msg"]["interface_name"] = tincan_resp_msg["Request"]["InterfaceName"]

                        if "message_type" in iccmsg["msg"]:
                            # Check whether data routed is Packetdata
                            if iccmsg["msg"]["message_type"] == "BroadcastPkt":
                                dataframe = iccmsg["msg"]["dataframe"]
                                # Check whether the Packet is ARP Packet
                                if dataframe[24:28] == "0806":
                                    self.registerCBT('NodeDiscovery', 'ARPPacket', iccmsg["msg"])
                                # Check whether packet is IPMulticast Packet
                                elif dataframe[0:6] == "01005E":
                                    self.registerCBT('IPMulticast', 'multicast', iccmsg["msg"])
                                else:
                                    # Broadcast other packets
                                    self.registerCBT('BroadCastForwarder', 'BroadcastPkt', iccmsg["msg"])
                            # Check whether data is Control data
                            elif iccmsg["msg"]["message_type"] == "BroadcastData":
                                # Convert string control data to dictionary
                                iccmessage = ast.literal_eval(iccmsg["msg"]["dataframe"])
                                iccmessage["interface_name"] = tincan_resp_msg["Request"]["InterfaceName"]
                                # Check the control data message type so that it routes to appropriate module
                                if iccmessage["message_type"] == "SendMacDetails":
                                    self.registerCBT('NodeDiscovery', 'PeerMACIPDetails', iccmessage)
                                self.registerCBT('BroadCastForwarder', 'BroadcastData', iccmsg["msg"])
                            else:
                                self.registerCBT('BaseTopologyManager', 'ICC_CONTROL', iccmsg["msg"])
                        else:
                            self.registerCBT('BroadCastForwarder', 'BroadcastData', iccmsg["msg"])
                    else:
                        # Pass all non routing messages to BTM
                        iccmsg["interface_name"] = tincan_resp_msg["Request"]["InterfaceName"]
                        self.registerCBT('BaseTopologyManager', 'ICC_CONTROL', iccmsg)

                elif req_operation == "UpdateRoutes":
                    # Register CBT that initialize BroadcastForwarder with latest online peer list
                    self.registerCBT('ConnectionManager', 'TINCAN_CONTROL', req_peer_list)

                    msg = tincan_resp_msg["Request"]["Data"]
                    interface_name = tincan_resp_msg["Request"]["InterfaceName"]
                    # Create message for inter module communication within the controller
                    datagram = {
                        "dataframe": msg,
                        "interface_name": interface_name,
                        "type": "local"
                    }

                    log = "UpdateRoutes Message ::{0}".format(datagram)
                    self.registerCBT('Logger', 'debug', log)

                    # Check whether Packet is IP message then route it to BTM's packet handling method
                    if str(msg[24:28]) == "0800":
                        datagram["m_type"] = "IP"
                        self.registerCBT("BaseTopologyManager", "TINCAN_PACKET", datagram)
                    # Check whether Packet is ARP message then route to ARPManager
                    elif str(msg[24:28]) == "0806":
                        datagram["m_type"] = "ARP"
                        self.registerCBT('NodeDiscovery', 'ARPPacket', datagram)
                    # Send all other Packets for Broadcast
                    else:
                        datagram["message_type"] = "BroadcastPkt"
                        self.registerCBT('BroadCastForwarder', 'BroadcastPkt', datagram)
                else:
                    log = '{0}: unrecognized Data {1} received from {2}. Data:::{3}' \
                        .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
                    self.registerCBT('Logger', 'warning', log)
        else:
            log = '{0}: unrecognized CBT {1} received from {2}'\
                    .format(cbt.recipient, cbt.action, cbt.initiator)
            self.registerCBT('Logger', 'warning', log)

    def send_msg(self, msg):
        return self.sock.sendto(bytes((msg).encode('utf-8')), self.dest)

    def timer_method(self):
        pass

    def terminate(self):
        pass

