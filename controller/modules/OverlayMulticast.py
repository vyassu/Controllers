from controller.framework.ControllerModule import ControllerModule
import binascii


class OverlayMulticast(ControllerModule):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        super(OverlayMulticast, self).__init__(CFxHandle, paramDict, ModuleName)
        self.ConfigData = paramDict
        self.overlay_multicast_details={}
        self.tincanparams = self.CFxHandle.queryParam("VirtualNetworkInitializer", "Vnets")
        for k in range(len(self.tincanparams)):
            interface_name  = self.tincanparams[k]["TapName"]
            self.overlay_multicast_details[interface_name] = {}
            self.overlay_multicast_details[interface_name]["uid"] = self.tincanparams[k]["uid"]
            self.overlay_multicast_details[interface_name]["publish"] = {}
            self.overlay_multicast_details[interface_name]["subscription"] = {}
        self.tincanparams =None

    def initialize(self):
        self.registerCBT('Logger', 'info', "{0} Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        self.registerCBT("Logger", "debug", "Inside Overlay Multicast:: {0}".format(str(cbt.data)))
        interface_name = cbt.data["interface_name"]
        dataframe = cbt.data["dataframe"]

        operation = str(binascii.unhexlify(dataframe[76:80]))
        self.registerCBT("Logger", "info", "Operation::{0}".format(operation))
        topiclength = int(binascii.unhexlify(dataframe[80:84])) * 2
        topic = dataframe[84:84 + topiclength]
        self.registerCBT("Logger", "info", "Topic:: {0}".format(binascii.unhexlify(topic)))

        if operation == "01":
            if cbt.data.get("type") == "local":
                if topic not in self.overlay_multicast_details[interface_name]["publish"].keys():
                    self.overlay_multicast_details[interface_name]["publish"][topic] = []
                self.registerCBT("Logger", "info", "Modified Dataframe {0}".format(str(dataframe)))
                msg = {
                    "interface_name" : interface_name,
                    "dataframe": dataframe,
                    "type": "local"
                }
                self.registerCBT("BroadCastForwarder","BroadcastPkt",msg)
            else:
                publish_uid = cbt.data.get("init_uid")
                if topic not in self.overlay_multicast_details[interface_name]["subscription"].keys():
                    self.overlay_multicast_details[interface_name]["subscription"][topic] = [publish_uid]
                else:
                    self.overlay_multicast_details[interface_name]["subscription"][topic].append(publish_uid)
                    self.overlay_multicast_details[interface_name]["subscription"][topic] = \
                        list(set(self.overlay_multicast_details[interface_name]["subscription"][topic]))
                self.registerCBT("BroadCastForwarder", "BroadcastPkt", cbt.data)
            self.registerCBT("Logger", "debug",
                             "Multicast Table:::" + str(self.overlay_multicast_details[interface_name]))
        elif operation in ["02","03"]:
            if cbt.data.get("type") == "remote":
                if topic in self.overlay_multicast_details[interface_name]["publish"].keys():
                    multicast_src_uid = cbt.data.get("src_uid")
                    # Subscribe Message
                    if operation == "02":
                        self.overlay_multicast_details[interface_name]["publish"][topic].append(multicast_src_uid)
                        self.overlay_multicast_details[interface_name]["publish"][topic] = \
                            list(set(self.overlay_multicast_details[interface_name]["publish"][topic]))
                    # Unsubscribe Message
                    else:
                        if multicast_src_uid in self.overlay_multicast_details[interface_name]["publish"][topic]:
                            self.overlay_multicast_details[interface_name]["publish"][topic].remove(multicast_src_uid)
            else:
                if topic in self.overlay_multicast_details[interface_name]["subscription"].keys():
                    multicast_src_list = self.overlay_multicast_details[interface_name]["subscription"][topic]
                    for dst_uid in multicast_src_list:
                        new_msg = {
                            "msg_type": "forward",
                            "src_uid": self.overlay_multicast_details[interface_name]["uid"],
                            "dst_uid": dst_uid,
                            "interface_name": interface_name,
                            "datagram": dataframe
                        }

                        self.registerCBT("BaseTopologyManager", "ICC_CONTROL", new_msg)
                else:
                    self.registerCBT("BroadCastForwarder", "BroadcastPkt", cbt.data)
            self.registerCBT("Logger", "debug", "Multicast Table:::" + str(self.overlay_multicast_details[interface_name]))
        else:
            self.registerCBT("Logger", "debug", "Overlay Data" + str(cbt.data))
            if topic in self.overlay_multicast_details[interface_name]["publish"].keys():
                multicast_dst_list = self.overlay_multicast_details[interface_name]["publish"][topic]
                self.registerCBT("Logger", "info", "Multicast candidate list::" + str(multicast_dst_list))

                for dst_uid in multicast_dst_list:
                    new_msg = {
                        "msg_type": "forward",
                        "src_uid": self.overlay_multicast_details[interface_name]["uid"],
                        "dst_uid": dst_uid,
                        "interface_name": interface_name,
                        "datagram": dataframe
                    }

                    self.registerCBT("BaseTopologyManager", "ICC_CONTROL", new_msg)

    def timer_method(self):
        pass

    def terminate(self):
        pass