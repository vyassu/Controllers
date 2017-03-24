import json,ast
from controller.framework.ControllerModule import ControllerModule


class TincanDispatcher(ControllerModule):

    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(TincanDispatcher, self).__init__(CFxHandle, paramDict, ModuleName)

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def timer_method(self):
        pass

    def terminate(self):
        pass

    def processCBT(self, cbt):
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

                        # Check whether the Query is for Self or a remote node
                        if resp_msg["Type"] == "local":
                            msg = {
                                    "type":"local_state",
                                    "_uid": resp_msg["UID"],
                                    "_ip4": resp_msg["VIP4"],
                                    #"_ip6": resp_msg["VIP6"],
                                    "_fpr": resp_msg["Fingerprint"],
                                    "mac" : resp_msg["MAC"],
                                    "interface_name":interface_name
                                }
                            log = "current state of {0} : {1}".format(resp_msg["UID"], str(msg))
                            self.registerCBT('Logger', 'debug', log)
                            self.registerCBT('BaseTopologyManager', 'TINCAN_CONTROL', msg)
                        else:
                            # Checks whether the link to peer is in Unknown state
                            if resp_msg["Status"]!="unknown":
                                msg = {
											"type": "peer_state",
											"uid": tincan_resp_msg["Request"]["UID"],
											"ip4": resp_msg["VIP4"],
											#"ip6": resp_msg["VIP6"],
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
                                    "ip6": "",
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
                            self.registerCBT('BaseTopologyManager', 'TINCAN_CONTROL', msg)

                elif req_operation == "CreateLinkListener":
                    # Sends the CAS to BaseTopologyManger
                    log = "recv data from Tincan for operation: {0}".format(tincan_resp_msg["Request"]["Command"])
                    self.registerCBT('Logger', 'info', log)
                    self.registerCBT('Logger', 'debug', "Message: "+str(tincan_resp_msg))
                    msg = {
                        "type"  : "con_ack",
                        "uid"   : tincan_resp_msg["Request"]["PeerInfo"]["UID"],
                        "data"  :  {
                                    "fpr"   : tincan_resp_msg["Request"]["PeerInfo"]["Fingerprint"],
                                    "cas"   : tincan_resp_msg['Response']['Message'],
                                    "con_type" : tincan_resp_msg["Request"]["PeerInfo"]["con_type"],
                                    "peer_mac" : tincan_resp_msg["Request"]["PeerInfo"]["MAC"]
                        },
                        "interface_name": interface_name
                    }
                    self.registerCBT('BaseTopologyManager', 'TINCAN_CONTROL', msg)
                elif req_operation == "ConnectToPeer":
                    # Response message for Connection Request for a link
                    log = "recv data from Tincan for operation: {0}".format(tincan_resp_msg["Request"]["Command"])
                    self.registerCBT('Logger', 'info', log)
                    self.registerCBT('Logger', 'debug', "Message: " + str(tincan_resp_msg))
                    msg = {
                        "type": "con_resp",
                        "uid" : tincan_resp_msg["Request"]["PeerInfo"]["UID"],
                        "data": {
                            "fpr": tincan_resp_msg["Request"]["PeerInfo"]["Fingerprint"],
                            "cas": tincan_resp_msg["Request"]["PeerInfo"]["CAS"],
                            "con_type": tincan_resp_msg["Request"]["PeerInfo"]["con_type"]
                        },
                        "status": "offline",
                        "interface_name": interface_name
                    }
                    #self.registerCBT('BaseTopologyManager', 'TINCAN_CONTROL', msg)

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

                self.registerCBT('BaseTopologyManager', 'TINCAN_CONTROL', req_peer_list)
                self.registerCBT('Logger', 'debug', "ICC Message Received ::"+str(iccmsg))

                # Checks whether ICC message is routing related
                if "msg" in iccmsg.keys():
                    iccmsg["msg"]["type"] = "remote"
                    iccmsg["msg"]["interface_name"] = tincan_resp_msg["Request"]["InterfaceName"]

                    if "message_type" in iccmsg["msg"]:
                        # Check whether data routed is Packetdata
                        if iccmsg["msg"]["message_type"] == "BroadcastPkt":
                            dataframe = iccmsg["msg"]["dataframe"]
                            #Check whether the Packet is ARP Packet
                            if dataframe[24:28] == "0806":
                                self.registerCBT('NodeDiscovery', 'ARPPacket', iccmsg["msg"])
                            # Check whether packet is IPMulticast Packet
                            elif dataframe[0:6] == "01005E":
                                self.registerCBT('IPMulticast', 'multicast', iccmsg["msg"])
                            # Check whether packet is OverlayMulticast Packet
                            elif dataframe[68:76] == "49504F50":
                                self.registerCBT("OverlayMulticast", "multicast", iccmsg["msg"])
                            else:
                                # Broadcast other packets
                                self.registerCBT('BroadCastController', 'BroadcastPkt', iccmsg["msg"])
                        # Check whether data is Control data
                        elif iccmsg["msg"]["message_type"] == "BroadcastData":
                            # Convert string control data to dictionary
                            iccmessage = ast.literal_eval(iccmsg["msg"]["dataframe"])
                            iccmessage["interface_name"] = tincan_resp_msg["Request"]["InterfaceName"]
                            # Check the control data message type so that it routes to appropriate module
                            if iccmessage["message_type"] == "SendMacDetails":
                                self.registerCBT('NodeDiscovery', 'PeerMACIPDetails', iccmessage)
                            self.registerCBT('BroadCastController', 'BroadcastData',iccmsg["msg"])
                        else:
                            self.registerCBT('BaseTopologyManager', 'ICC_CONTROL', iccmsg["msg"])
                    else:
                        self.registerCBT('BroadCastController', 'BroadcastData', iccmsg["msg"])
                else:
                    # Pass all non routing messages to BTM
                    iccmsg["interface_name"] = tincan_resp_msg["Request"]["InterfaceName"]
                    self.registerCBT('BaseTopologyManager', 'ICC_CONTROL', iccmsg)

            elif req_operation == "UpdateRoutes":
                # Register CBT that initialize BroadcastForwarder with latest online peer list
                self.registerCBT('BaseTopologyManager', 'TINCAN_CONTROL',req_peer_list)

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
                    self.registerCBT('BroadCastController', 'BroadcastPkt', datagram)
            else:
                log = '{0}: unrecognized Data {1} received from {2}. Data:::{3}' \
                    .format(cbt.recipient, cbt.action, cbt.initiator,cbt.data)
                self.registerCBT('Logger', 'warning', log)
