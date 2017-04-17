from controller.framework.ControllerModule import ControllerModule

class NodeDiscovery(ControllerModule):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(NodeDiscovery, self).__init__(CFxHandle, paramDict, ModuleName)
        self.ConfigData = paramDict
        # Query CFX to get properties of virtual networks configured by the user
        self.tincanparams = self.CFxHandle.queryParam("VirtualNetworkInitializer","Vnets")
        self.ipop_interface_details = {}
        #Iterate across the virtual networks to get UID,IP4 and TAPName
        for k in range(len(self.tincanparams)):
            interface_name  = self.tincanparams[k]["TapName"]
            self.ipop_interface_details[interface_name] = {}
            interface_detail                            = self.ipop_interface_details[interface_name]
            interface_detail["uid"]                     = self.tincanparams[k]["uid"]
            interface_detail["msgcount"]                = {}
            interface_detail["mac"]                     = ""
            interface_detail["ip"]                      = self.tincanparams[k]["IP4"]
            interface_detail["local_mac_ip_table"]      = {}
        # Clear the copy of network details from CFX after loading
        self.tincanparams = None

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        frame               = cbt.data.get("dataframe")
        interface_name      = cbt.data["interface_name"]
        interface_details   = self.ipop_interface_details[interface_name]
        srcmac,destmac,srcip,destip = "","","",""

        # Populate Local UID's MAC details. Data is send by BTM
        if cbt.action == "getlocalmacaddress":
            self.ipop_interface_details[interface_name]["mac"] = cbt.data.get("localmac")
            return
        # Process UID-MAC-IP details from other nodes in the network
        elif cbt.action == "PeerMACIPDetails":
            self.registerCBT('Logger', 'debug', "Multicast Message:: "+str(cbt.data))

            mac_ip_table    = cbt.data["mac_ip_table"]
            src_uid         = cbt.data["src_uid"]

            # Message for BTM to update the master UID-MAC-IP Tables
            UpdateBTMMacUIDTable = {
                "uid"               : src_uid,
                "mac_ip_table"      : mac_ip_table,
                "interface_name"    : interface_name,
                "location"          : "remote",
                "type"              : "UpdateMACUIDIp"
            }
            self.registerCBT('BaseTopologyManager', 'TINCAN_CONTROL', UpdateBTMMacUIDTable)
            return
        # Process ARP Packets received
        elif cbt.action=="ARPPacket":
            self.registerCBT('Logger', 'info', "Inside ARP Manager Module")
            self.registerCBT('Logger', 'debug', "Message from {0}. Data: {1}".format(cbt.initiator,str(cbt.data)))

            maclen      = int(frame[36:38],16)
            iplen       = int(frame[38:40],16)
            op          = int(frame[40:44],16)
            srcmacindex = 44 + 2 * maclen
            srcmac      = frame[44:srcmacindex]
            srcipindex  = srcmacindex + 2 * iplen
            srcip       =  '.'.join(str(int(i, 16)) for i in [frame[srcmacindex:srcipindex][i:i+2] for i in range(0, 8, 2)])
            destmacindex= srcipindex + 2 * maclen
            destmac     = frame[srcipindex:destmacindex]
            destipindex = destmacindex + 2 * iplen
            destip      = '.'.join(str(int(i, 16)) for i in [frame[destmacindex:destipindex][i:i+2] for i in range(0, 8, 2)])

            self.registerCBT('Logger', 'debug', "Source MAC:: "+ str(srcmac))
            self.registerCBT('Logger', 'debug', "Source IP Address::  " + str(srcip))
            self.registerCBT('Logger', 'debug', "Destination MAC:: " + str(destmac))
            self.registerCBT('Logger', 'debug', "Destination IP Address:: " + str(destip))

        local_uid = interface_details["uid"]
        # ARP Request Packet
        if op == 1:
            # Check whether ARP Message is from local unmanaged nodes
            if cbt.data["type"] == "local":
                mac_ip_table = {}
                # Update Local MAC-IP Table with Unmanaged node MAC and IP details
                if int(srcmac,16) != 0:
                    interface_details["local_mac_ip_table"][srcmac] = srcip
                    mac_ip_table[srcmac] = srcip
                UpdateBTMMacUIDTable = {
                    "uid"         : local_uid,
                    "mac_ip_table": mac_ip_table,
                    "interface_name": interface_name,
                    "location": "local",
                    "type": "UpdateMACUIDIp"
                }

            else:
                uid = cbt.data["init_uid"]          # Get the remote control UID
                mac_ip_table = {}
                if int(srcmac, 16) != 0:
                    mac_ip_table[srcmac] = srcip

                UpdateBTMMacUIDTable = {
                    "uid"               : uid,
                    "mac_ip_table"      : mac_ip_table,
                    "interface_name"    : interface_name,
                    "location"          : "remote",
                    "type"              : "UpdateMACUIDIp"
                }

            # Update BTM MAC-UID-IP Tables
            self.registerCBT('BaseTopologyManager', 'TINCAN_CONTROL', UpdateBTMMacUIDTable)

            # Broadcast the ARP Message using the Overlay
            if destip != self.ipop_interface_details[interface_name]["ip"]:
                self.registerCBT('BroadCastForwarder', 'BroadcastPkt', cbt.data)
            elif destip == self.ipop_interface_details[interface_name]["ip"] and srcip == "0.0.0.0":
                self.registerCBT('BroadCastForwarder', 'BroadcastPkt', cbt.data)
            elif destmac in list(self.ipop_interface_details[interface_name]["local_mac_ip_table"].keys()):
                self.registerCBT('TincanInterface', 'DO_INSERT_DATA_PACKET', cbt.data)
            else:
                self.registerCBT('TincanInterface', 'DO_INSERT_DATA_PACKET', cbt.data)
                self.registerCBT('BroadCastForwarder', 'BroadcastPkt', cbt.data)


        # ARP Reply Packet: Send ARP Reply as unicast to the source and Broadcast local MAC-IP Table for setting up routing rules
        # in the Tincan
        else:
            if int(srcmac, 16) != 0:
                interface_details["local_mac_ip_table"][srcmac] = srcip
            # Send ARP Reply as a unicast packet
            self.registerCBT('BaseTopologyManager', 'TINCAN_PACKET', cbt.data)

            sendlocalmacdetails = {
                        "interface_name": interface_name,
                        "type"          : "local",
                        "src_uid"       : local_uid,
                        "dataframe"     : {
                                "src_uid"       : local_uid,
                                "src_node_mac"  : interface_details["mac"],
                                "mac_ip_table"  : interface_details["local_mac_ip_table"],
                                "message_type"  : "SendMacDetails"
                        }
            }
            # Send Local MAC-IP Table for setting up routing rules.
            self.registerCBT('Logger', 'debug', "Sending Local/Peer MAC details:: "+str(sendlocalmacdetails))
            self.registerCBT('BroadCastForwarder', 'BroadcastData', sendlocalmacdetails)

    def terminate(self):
        pass

    def timer_method(self):
        pass