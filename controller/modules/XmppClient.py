#!/usr/bin/env python
import sys,ssl,time
from controller.framework.ControllerModule import ControllerModule
from collections import defaultdict

try:
    import sleekxmpp
    from sleekxmpp.xmlstream.stanzabase import ElementBase, JID
    from sleekxmpp.xmlstream import register_stanza_plugin
    from sleekxmpp.xmlstream.handler.callback import Callback
    from sleekxmpp.xmlstream.matcher import StanzaPath
    from sleekxmpp.stanza.message import Message
except:
    raise ImportError("Sleekxmpp Module not installed")

py_ver = sys.version_info[0]
if py_ver == 3:
    import _thread as thread
else:
    import thread,threading
log_level = "info"

class XmppClient(ControllerModule):
    def __init__(self, CFxHandle, paramDict, ModuleName):
        ControllerModule.__init__(self, CFxHandle, paramDict, ModuleName)

        xmpp_details = self.CMConfig.get("xmppdetails")
        self.ipop_xmpp_details = {}
        for i,xmpp_ele in enumerate(xmpp_details):
            xmpp_ele = dict(xmpp_ele)
            interface_name  = xmpp_ele['TapName']
            self.ipop_xmpp_details[interface_name] = {}
            if xmpp_ele.get("AuthenticationMethod") == "x509" and (xmpp_ele.get("Username",None) != None or \
                        xmpp_ele.get("Password", None) != None):
                raise RuntimeError("x509 Authentication Error: Username/Password in IPOP configuration file.")

            if xmpp_ele.get("AuthenticationMethod") == "x509":
                xmppObj = sleekxmpp.ClientXMPP(xmpp_ele['Username'],xmpp_ele['Password'],sasl_mech='EXTERNAL')
                #sleekxmpp.ClientXMPP.__init__(self, self.xmpp_host, self.xmpp_passwd, sasl_mech='EXTERNAL')
                xmppObj.ssl_version = ssl.PROTOCOL_TLSv1                                #self.ssl_version = ssl.PROTOCOL_TLSv1
                xmppObj.ca_certs    = xmppObj["TrustStore"]                             #self.ca_certs = self.CMConfig.get("TrustStore")
                xmppObj.certfile    = xmppObj["CertDirectory"] + xmppObj["CertFile"]    #self.certfile = self.CMConfig.get("CertDirectory") + self.CMConfig.get("CertFile")
                xmppObj.keyfile     = xmppObj["CertDirectory"] + xmppObj["Keyfile"]     #self.keyfile = self.CMConfig.get("CertDirectory") + self.CMConfig.get("Keyfile")
                xmppObj.use_tls     = True
            else:
                xmppObj = sleekxmpp.ClientXMPP(xmpp_ele['Username'], xmpp_ele['Password'], sasl_mech='PLAIN')
                #sleekxmpp.ClientXMPP.__init__(self, self.xmpp_username, self.xmpp_passwd, sasl_mech='PLAIN')
                if xmpp_ele.get("AcceptUntrustedServer") == True:
                    xmppObj.register_plugin("feature_mechanisms",pconfig={'unencrypted_plain': True})   #self['feature_mechanisms'].unencrypted_plain = True
                    xmppObj.use_tls = False
                else:
                    xmppObj.ca_certs = xmpp_ele["TrustStore"]

            register_stanza_plugin(Message, Ipop_Msg)
            xmppObj.registerHandler(Callback('Ipop', StanzaPath('message/Ipop'), self.MsgListener))
            # Register event handler for session start
            xmppObj.add_event_handler("session_start", self.start)      #self.add_event_handler("session_start", self.start)
            xmppObj.add_event_handler("roster_update", self.deletepeerjid) #self.add_event_handler("roster_update", self.deletepeerjid)

            self.ipop_xmpp_details[interface_name]["XMPPObj"]    = xmppObj
            self.ipop_xmpp_details[interface_name]["username"]   = xmpp_ele["Username"]
            self.ipop_xmpp_details[interface_name]["xmpp_peers"] = defaultdict(lambda: [0, False])
            self.ipop_xmpp_details[interface_name]["uid_jid"]    = {}
            self.ipop_xmpp_details[interface_name]["jid_uid"]    = defaultdict(lambda: ['', False, 1])
            self.ipop_xmpp_details[interface_name]["callbackinit"] = False
            self.ipop_xmpp_details[interface_name]["last_sent_advt"] = 0
            # keeps track of if xmpp advt recvd in interval
            self.ipop_xmpp_details[interface_name]["xmpp_advt_recvd"] = True
            # Initial ADVT Delay
            self.ipop_xmpp_details[interface_name]["INITIAL_ADVT_DELAY"] = 5
            # interval between sending advertisements
            self.ipop_xmpp_details[interface_name]["advt_delay"] = 5
            # Maximum delay between advertisements is 10 minutes
            self.ipop_xmpp_details[interface_name]["MAX_ADVT_DELAY"] = 120

            ipop_interfaces = self.CFxHandle.queryParam("VirtualNetworkInitializer", "Vnets")
            for interface_details in ipop_interfaces:
                if interface_details["TapName"] == interface_name:
                    self.ipop_xmpp_details[interface_name]["uid"] = interface_details["uid"]
            self.xmpp_handler(xmpp_ele,xmppObj)

    # Triggered at start of XMPP session
    def start(self, event):
        try:
            for xmpp_detail in list(self.ipop_xmpp_details.values()):
                if xmpp_detail["callbackinit"] == False:
                    xmpp_detail["callbackinit"]=True
                    xmpp_detail["XMPPObj"].get_roster()
                    xmpp_detail["XMPPObj"].send_presence()
                    xmpp_detail["XMPPObj"].add_event_handler("presence_available", self.handle_presence)
                    xmpp_detail["XMPPObj"].add_event_handler("presence_unavailable", self.removepeerjid)
        except Exception as err:
            self.log("Exception in XMPPClient:".format(err), severity="error")

    # will need to handle presence, to keep track of who is online.
    def handle_presence(self, presence):
        try:
            presence_sender = presence['from']
            presence_receiver_jid = JID(presence['to'])
            presence_receiver = str(presence_receiver_jid.user)+"@"+str(presence_receiver_jid.domain)
            for xmpp_details in self.ipop_xmpp_details.values():
                if presence_receiver == xmpp_details["username"]:
                    if xmpp_details["xmpp_peers"][presence_sender][1] == False:
                        xmpp_details["xmpp_peers"][presence_sender] = [time.time(), True]
                        self.log("presence received from {0}".format(presence_sender), severity=log_level)
        except Exception as err:
            self.log("Exception in XMPPClient:".format(err), severity="error")

    # Call Remove connection once the Peer has been deleted from the friend list(Roster)
    def deletepeerjid(self,message):
        try:
            self.log("XMPP server Message::"+str(message))
            for nodejid,data in message["roster"]["items"].items():
                if data["subscription"] == "remove":
                    for ele in self.jid_uid.keys():
                        tempjid = JID(ele)
                        jid = str(tempjid.user)+"@"+str(tempjid.domain)
                        if jid.find(str(nodejid)) !=-1:
                            node_uid = self.jid_uid[ele][0]
                            del self.jid_uid[ele]
                            del self.xmpp_peers[ele]
                            if node_uid in self.uid_jid.keys():
                                del self.uid_jid[node_uid]
                                self.update_peerlist = True
                                self.registerCBT("Logger","info","{0} has been deleted from the roster.".format(node_uid))
                                self.registerCBT("ConnectionManager","remove_connection",\
                                                 {"interface_name":self.interface_name,"uid":node_uid})
                                msg = {
                                    "uid": node_uid,
                                    "type": "offline_peer",
                                    "interface_name": self.interface_name
                                }
                                self.registerCBT("BaseTopologyManager", "XMPP_MSG", msg)

        except Exception as err:
            self.log("Exception in deletepeerjid method.{0}".format(err),severity="error")

    # Remove the Offline Peer from the internal dictionary
    def removepeerjid(self,message):
        try:
            peerjid = message["from"]
            self.log("Peer JID {0} offline".format(peerjid))

            presence_receiver_jid = JID(message['to'])
            presence_receiver = str(presence_receiver_jid.user) + "@" + str(presence_receiver_jid.domain)
            interface_name =""
            for tapName,xmpp_details in list(self.ipop_xmpp_details.items()):
                if presence_receiver == xmpp_details["username"]:
                    xmppObj = xmpp_details
                    interface_name = tapName
                    break
            if peerjid in xmppObj["xmpp_peers"].keys():
                del xmppObj["xmpp_peers"][peerjid]

            if peerjid in xmppObj["jid_uid"].keys():
                uid = xmppObj["jid_uid"][peerjid][0]
                del xmppObj["jid_uid"][peerjid]
                if uid in xmppObj["uid_jid"].keys():
                    del xmppObj["uid_jid"][uid]
                    #self.update_peerlist = True
                    self.registerCBT("BaseTopologyManager", "UpdateXMPPPeer",
                                     {"update_peerlist": True, "interface_name": interface_name})
                    self.registerCBT("ConnectionManager", "remove_connection", \
                                     {"interface_name": interface_name, "uid": uid})
                    self.log("Removed Peer JID: {0} UID: {1} from the JID-UID and UID-JID Table".format(peerjid, uid))
                    msg = {
                        "uid": uid,
                        "type": "offline_peer",
                        "interface_name": interface_name
                    }
                    self.registerCBT("BaseTopologyManager", "XMPP_MSG", msg)
        except Exception as err:
            self.log("Exception in remove peerjid method. Error::{0}".format(err), severity="error")

    # This handler method listens for the matched messages on tehj xmpp stream,
    # extracts the setup and payload and takes suitable action depending on the
    # them.
    def MsgListener(self, msg):
        presence_receiver_jid = JID(msg['to'])
        presence_receiver = str(presence_receiver_jid.user) + "@" + str(presence_receiver_jid.domain)
        interface_name = ""
        for tapName, xmpp_details in list(self.ipop_xmpp_details.items()):
            if presence_receiver == xmpp_details["username"]:
                xmppObj = xmpp_details
                interface_name = tapName
                break

        if xmppObj["uid"] == "":
            self.log("UID not received from Tincan. Please check Tincan logs.",severity="warning")
            return

        # extract setup and content
        setup = str(msg['Ipop']['setup'])
        payload = str(msg['Ipop']['payload'])
        msg_type, target_uid, target_jid = setup.split("#")
        sender_jid = msg['from']

        if (msg_type == "regular_msg"):
            self.log("Recvd mesage from {0}".format(msg['from']), severity=log_level)
            self.log("Msg is {0}".format(payload), severity="debug")
        elif (msg_type == "xmpp_advertisement"):
            # peer_uid - uid of the node that sent the advt
            # target_uid - what it percieves as my uid
            try:
                peer_uid, target_uid = payload.split("#")
                print(peer_uid)
                print(xmppObj["uid"])
                if peer_uid != xmppObj["uid"]:
                    if peer_uid not in xmppObj["uid_jid"].keys():
                        # self.update_peerlist= True
                        self.registerCBT("BaseTopologyManager", "UpdateXMPPPeer", \
                                         {"update_peerlist": True, "interface_name": interface_name})
                    # update last known advt reception time in xmpp_peers
                    xmppObj["xmpp_peers"][sender_jid][0] = time.time()
                    xmppObj["uid_jid"][peer_uid] = sender_jid
                    xmppObj["jid_uid"][msg['from']][0] = peer_uid
                    # sender knows my uid, so I will not send an advert to him
                    if target_uid == xmppObj["uid"]:
                        xmppObj["jid_uid"][msg['from']][1] = True
                        # recvd correct advertisement
                        xmppObj["jid_uid"][msg['from']][2] += 1
                    else:
                        xmppObj["jid_uid"][msg['from']][1] = False
                    # refresh xmpp advt recvd flag
                    self.ipop_xmpp_details[interface_name]["xmpp_advt_recvd"] = True
                    self.log("recvd xmpp_advt from {0}".format(peer_uid), severity=log_level)

            except:
                self.log("advt_payload: {0}".format(payload), severity="error")

        # compare uid's here , if target uid does not match with mine do nothing.
        # have to avoid loop messages.
        if target_uid == xmppObj["uid"]:
            sender_uid, recvd_data = payload.split("#")
            # If I recvd XMPP msg from this peer, I should record his UID-JID & JID-UID
            if sender_uid not in xmppObj["uid_jid"].keys():
                # self.update_peerlist = True
                self.registerCBT("BaseTopologyManager", "UpdateXMPPPeer", \
                                 {"update_peerlist": True, "interface_name": interface_name})
            xmppObj["uid_jid"][sender_uid] = sender_jid

            if (msg_type == "con_req"):
                msg = {}
                msg["uid"] = sender_uid
                msg["data"] = recvd_data
                msg["type"] = "con_req"
                msg["interface_name"] = interface_name
                # send this CBT to BaseTopology Manager
                self.registerCBT('ConnectionManager', 'respond_connection', msg)
                self.log("recvd con_req from {0}".format(msg["uid"]), severity=log_level)

            elif (msg_type == "con_ack"):
                msg = {}
                msg["uid"] = sender_uid
                msg["data"] = recvd_data
                msg["type"] = "con_ack"
                msg["interface_name"] = interface_name
                self.registerCBT('ConnectionManager', 'create_connection', msg)
                self.log("recvd con_ack from {0}".format(msg["uid"]), severity=log_level)

            elif (msg_type == "con_resp"):
                msg = {}
                msg["uid"] = sender_uid
                msg["data"] = recvd_data
                msg["type"] = "peer_con_resp"
                msg["interface_name"] = interface_name
                self.registerCBT('BaseTopologyManager', 'XMPP_MSG', msg)
                self.log("recvd con_resp from {0}".format(msg["uid"]), severity=log_level)

            '''
            elif (msg_type == "ping_resp"):
                msg = {}
                msg["uid"] = sender_uid
                msg["data"] = recvd_data
                msg["type"] = "ping_resp"
                msg["interface_name"] = interface_name
                self.registerCBT('BaseTopologyManager', 'XMPP_MSG', msg)
                self.log("recvd ping_resp from {0}".format(msg["uid"]), severity=log_level)

            elif (msg_type == "ping"):
                msg = {}
                msg["uid"] = sender_uid
                msg["data"] = recvd_data
                msg["type"] = "ping"
                msg["interface_name"] = interface_name
                self.registerCBT('BaseTopologyManager', 'XMPP_MSG', msg)
                self.log("recvd ping from {0}".format(msg["uid"]), severity=log_level)
            '''

    def sendMsg(self, peer_jid, xmppObj,setup_load=None, msg_payload=None):
        if (setup_load == None):
            setup_load = "regular_msg" + "#" + "None" + "#" + peer_jid.full
        else:
            setup_load = setup_load + "#" + peer_jid.full

        if py_ver != 3:
            setup_load = unicode(setup_load)

        if (msg_payload == None):
            content_load = "Hello there this is {0}".format(xmppObj.username)
        else:
            content_load = msg_payload

        msg = xmppObj.Message()
        msg['to'] = peer_jid.bare
        msg['type'] = 'chat'
        msg['Ipop']['setup'] = setup_load
        msg['Ipop']['payload'] = content_load
        msg.send()
        self.log("Sent a message to  {0}".format(peer_jid), severity=log_level)

    def xmpp_handler(self,xmpp_details,xmppObj):
        try:
            if (xmppObj.connect(address=(xmpp_details["AddressHost"], xmpp_details["Port"]))):
                thread.start_new_thread(xmppObj.process, ())
                self.log("Started XMPP handling", severity="debug")
        except Exception as err:
            self.log("Unable to start XMPP handling thread-Check Internet connectivity/credentials."+str(err), severity='error')

    def log(self, msg, severity='info'):
        self.registerCBT('Logger', severity, msg)

    def initialize(self):
        self.log("{0} module Loaded".format(self.ModuleName))

    def processCBT(self, cbt):
        message = cbt.data
        interface_name = message.get("interface_name")
        if (cbt.action == "DO_SEND_MSG"):
            if self.ipop_xmpp_details[interface_name]["uid"] == "":
                self.log("UID not received from Tincan. Please check Tincan logs.", severity="warning")
                return
            method   = message.get("method")
            peer_uid = message.get("uid")
            node_uid = self.ipop_xmpp_details[interface_name]["uid"]
            if peer_uid in self.ipop_xmpp_details[interface_name]["uid_jid"].keys():
                peer_jid = self.ipop_xmpp_details[interface_name]["uid_jid"][peer_uid]
            else:
                log_msg = "UID-JID mapping for UID: {0} not present.\
                            msg: {1} will not be sent.".format(peer_uid, method)
                self.log(log_msg)
                return
            data = message.get("data")

            if (method == "con_req"):
                setup_load = "con_req" + "#" + peer_uid
                msg_payload = node_uid+ "#" + data
                self.sendMsg(peer_jid, self.ipop_xmpp_details[interface_name]["XMPPObj"],setup_load, msg_payload)
                self.log("sent con_req to {0}".format(self.ipop_xmpp_details[interface_name]["uid_jid"][peer_uid]), severity=log_level)
            elif (method == "con_resp"):
                setup_load = "con_resp" + "#" + peer_uid
                msg_payload = node_uid + "#" + data
                self.sendMsg(peer_jid, self.ipop_xmpp_details[interface_name]["XMPPObj"], setup_load, msg_payload)
                self.log("sent con_resp to {0}".format(self.ipop_xmpp_details[interface_name]["uid_jid"][peer_uid]), severity=log_level)
            elif (method == "con_ack"):
                setup_load = "con_ack" + "#" + peer_uid
                msg_payload = node_uid + "#" + data
                self.sendMsg(peer_jid, self.ipop_xmpp_details[interface_name]["XMPPObj"], setup_load, msg_payload)
                self.log("sent con_ack to {0}".format(self.ipop_xmpp_details[interface_name]["uid_jid"][peer_uid]), severity=log_level)
            elif (method == "ping_resp"):
                setup_load = "ping_resp" + "#" + peer_uid
                msg_payload = node_uid + "#" + data
                self.sendMsg(peer_jid, self.ipop_xmpp_details[interface_name]["XMPPObj"], setup_load, msg_payload)
                self.log("sent ping_resp to {0}".format(self.ipop_xmpp_details[interface_name]["uid_jid"][peer_uid]), severity=log_level)
            elif (method == "ping"):
                setup_load = "ping" + "#" + peer_uid
                msg_payload = node_uid + "#" + data
                self.sendMsg(peer_jid, self.ipop_xmpp_details[interface_name]["XMPPObj"], setup_load, msg_payload)
                self.log("sent ping to {0}".format(self.ipop_xmpp_details[interface_name]["uid_jid"][peer_uid]), severity=log_level)
        elif cbt.action == "GetXMPPPeer":
            msg = {
                    "interface_name": interface_name,
                    "peer_list"     : list(self.ipop_xmpp_details[interface_name]["uid_jid"].keys())
            }
            self.registerCBT(cbt.initiator, "UpdateXMPPPeer", msg)


    def sendXmppAdvt(self, interface_name, override=False):
        if self.ipop_xmpp_details[interface_name]["uid"] != "":
            for peer in self.ipop_xmpp_details[interface_name]["xmpp_peers"].keys():
                # True indicates that peer node does not knows my UID.
                # If I have recvd more than 10 correct advertisements from peer
                # reply back, may be my reply was lost.
                if self.ipop_xmpp_details[interface_name]["jid_uid"][peer][1] == True and \
                            self.ipop_xmpp_details[interface_name]["jid_uid"][peer][2] % 10 == 0:
                    send_advt = True
                    self.ipop_xmpp_details[interface_name]["jid_uid"][peer][2] = 1
                elif self.ipop_xmpp_details[interface_name]["jid_uid"][peer][1] == True and override != True:
                    # Do not send an advt
                    send_advt = False
                else:
                    # If here, peer does not knows my UID
                    send_advt = True

                if send_advt == True:
                    setup_load = "xmpp_advertisement" + "#" + "None"
                    msg_load = str(self.ipop_xmpp_details[interface_name]["uid"]) + "#" +\
                               str(self.ipop_xmpp_details[interface_name]["jid_uid"][peer][0])
                    self.sendMsg(peer, self.ipop_xmpp_details[interface_name]["XMPPObj"],setup_load, msg_load)
                    self.log("sent xmpp_advt to {0}".format(peer), severity=log_level)

    def timer_method(self):

        try:
            for interface_name in self.ipop_xmpp_details.keys():
                if (time.time() - self.ipop_xmpp_details[interface_name]["last_sent_advt"] > self.ipop_xmpp_details[interface_name]["advt_delay"]):
                    # see if I recvd a advertisement in this time period
                    # if yes than XMPP link is open
                    if self.ipop_xmpp_details[interface_name]["xmpp_advt_recvd"] == True:
                        self.sendXmppAdvt(interface_name=interface_name)
                        # update xmpp tracking parameters.
                        self.ipop_xmpp_details[interface_name]["last_sent_advt"] = time.time()
                        self.ipop_xmpp_details[interface_name]["xmpp_advt_recvd"] = False
                        self.ipop_xmpp_details[interface_name]["advt_delay"] = self.ipop_xmpp_details[interface_name]["INITIAL_ADVT_DELAY"]
                    # Have not heard from anyone in a while, Handles XMPP disconnection
                    # do not want to overwhelm with queued messages.
                    elif (self.ipop_xmpp_details[interface_name]["advt_delay"] < self.ipop_xmpp_details[interface_name]["MAX_ADVT_DELAY"]):
                        self.ipop_xmpp_details[interface_name]["advt_delay"] = 2 * self.ipop_xmpp_details[interface_name]["advt_delay"]
                        self.log("Delaying the XMPP advt timer \
                                to {0} seconds".format(self.ipop_xmpp_details[interface_name]["advt_delay"]))
                    else:
                        # send the advertisement anyway, after MaxDelay.
                        self.sendXmppAdvt(interface_name=interface_name,override=True)
                        # update xmpp tracking parameters.
                        self.ipop_xmpp_details[interface_name]["last_sent_advt"] = time.time()
                        self.ipop_xmpp_details[interface_name]["xmpp_advt_recvd"] = False
        except Exception as error:
            self.log("Exception in XmppClient timer.{0}".format(error.message), severity="error")

    def terminate(self):
        pass

# set up a new custom message stanza
class Ipop_Msg(ElementBase):
    namespace = "Conn_setup"
    name = 'Ipop'
    plugin_attrib = 'Ipop'
    interfaces = set(('setup', 'payload', 'uid','TapName'))
    subinterfaces = interfaces
