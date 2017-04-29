from controller.framework.ControllerModule import ControllerModule


class IPMulticast(ControllerModule):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(IPMulticast, self).__init__(CFxHandle, paramDict, ModuleName)
        self.ConfigData = paramDict
        self.multicast_details = {}
        self.tincanparams = self.CFxHandle.queryParam("VirtualNetworkInitializer", "Vnets")
        for k in range(len(self.tincanparams)):
            interface_name = self.tincanparams[k]["TapName"]
            self.multicast_details[interface_name] = {}
            self.multicast_details[interface_name]["uid"] = self.tincanparams[k]["uid"]
            self.multicast_details[interface_name]["Group"] = {}
        self.tincanparams = None

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def sendmulticastdata(self, dataframe, interface_name, multicast_address):
        self.registerCBT("Logger", "debug", "Multicast Data : {0}".format(str(dataframe)))
        if multicast_address in self.multicast_details[interface_name]["Group"].keys():
            multicast_dst_list = self.multicast_details[interface_name]["Group"][multicast_address]
            self.registerCBT("Logger", "info", "Multicast candidate list::" + str(multicast_dst_list))

            for dst_uid in multicast_dst_list:
                new_msg = {
                    "msg_type": "forward",
                    "src_uid": self.multicast_details[interface_name]["uid"],
                    "dst_uid": dst_uid,
                    "interface_name": interface_name,
                    "datagram": dataframe
                }
                self.registerCBT("BaseTopologyManager", "ICC_CONTROL", new_msg)

    def processCBT(self, cbt):
        self.registerCBT("Logger", "debug", "Inside IP Multicast:: {0}".format(str(cbt.data)))
        interface_name = cbt.data["interface_name"]
        dataframe = cbt.data["dataframe"]
        protocol = dataframe[46:48]
        headerlength = int(dataframe[29:30]) * 4
        
        # Check whether IP message contains IGMP as protocol
        if protocol == "02":
            # Store IGMP into a seperate variable for processing
            IGMPData = dataframe[28 + (headerlength * 2):]
            operation = IGMPData[1:2]
            # IGMP Request message
            if operation == "1":
                self.registerCBT("Logger", "info", "IGMP Group Membership Query")
                self.registerCBT("Logger", "debug", "Multicast Table::" + str(self.multicast_details[interface_name]))

                if cbt.data.get("type") == "local":
                    multicast_address = dataframe[-8:]
                    multicast_add = '.'.join(str(int(i, 16)) for i in [multicast_address[i: i + 2]
                                                                       for i in range(0, 8, 2)])
                    self.registerCBT("Logger", "debug", "Multicast Address::" + str(multicast_add))
                    if multicast_address not in self.multicast_details[interface_name]["Group"].keys():
                        self.multicast_details[interface_name]["Group"][multicast_address] = []
                    msg = {
                        "interface_name": interface_name,
                        "dataframe": dataframe,
                        "type": "local"
                    }
                    self.registerCBT("BroadCastForwarder", "BroadcastPkt", msg)
                else:
                    self.registerCBT("BroadCastForwarder", "BroadcastPkt", cbt.data)
            # IGMP Membership Report/Leave Group Message
            elif operation in ["2", "6", "7"]:
                self.registerCBT("Logger", "info", "IGMP Membership Report")
                multicast_address = dataframe[-8:]
                multicast_add = '.'.join(str(int(i, 16)) for i in [multicast_address[i:i + 2] for i in range(0, 8, 2)])
                self.registerCBT("Logger", "debug", "Multicast Address::" + str(multicast_add))
                if cbt.data.get("type") == "remote":
                    if multicast_address in self.multicast_details[interface_name]["Group"].keys():
                        multicast_src_uid = cbt.data.get("init_uid")
                        # IGMP Membership Report
                        if operation in ["2", "6"]:
                            self.multicast_details[interface_name]["Group"][multicast_address].append(multicast_src_uid)
                            self.multicast_details[interface_name]["Group"][multicast_address] = \
                                list(set(self.multicast_details[interface_name]["Group"][multicast_address]))
                        # IGMP Leave Group Message
                        else:
                            if multicast_src_uid in self.multicast_details[interface_name]["Group"][multicast_address]:
                                self.multicast_details[interface_name]["Group"][multicast_address].\
                                    remove(multicast_src_uid)
                    else:
                        self.registerCBT("BroadCastForwarder", "BroadcastPkt", cbt.data)
                else:
                    msg = {
                        "interface_name": interface_name,
                        "dataframe": dataframe,
                        "type": "local"
                    }
                    self.registerCBT("BroadCastForwarder", "BroadcastPkt", msg)
                self.registerCBT("Logger", "debug", "Multicast Table:::" + str(self.multicast_details[interface_name]))
            else:
                multicast_address = IGMPData[8:16]
                self.sendmulticastdata(dataframe, interface_name, multicast_address)
        else:
            multicast_address = dataframe[60:68]
            self.sendmulticastdata(dataframe, interface_name, multicast_address)

    def timer_method(self):
        pass

    def terminate(self):
        pass
