#!/usr/bin/env/ python
import sys,time
from controller.framework.ControllerModule import ControllerModule


py_ver = sys.version_info[0]
class BroadCastForwarder(ControllerModule):
    def __init__(self,CFxHandle, paramDict,ModuleName):
        super(BroadCastForwarder,self).__init__(CFxHandle,paramDict,ModuleName)
        self.uid=""
        self.ipop_interface_details = {}
        self.tincanparams = self.CFxHandle.queryParam("VirtualNetworkInitializer","Vnets")
        for k in range(len(self.tincanparams)):
            interface_name = self.tincanparams[k]["TapName"]
            self.ipop_interface_details[interface_name] = {}
            self.ipop_interface_details[interface_name]["uid"]       = self.tincanparams[k]["uid"]
            self.ipop_interface_details[interface_name]["mac"]       = ""
            self.ipop_interface_details[interface_name]["peerlist"] = []
        self.tincanparams = None
        self.prevtimestamp = []
        self.send_count = 0
        self.receive_count = 0
     
    def initialize(self):
        self.registerCBT('Logger','info',"{0} Loaded".format(self.ModuleName))

    def inserttimestamp(self,time):
        if len(self.prevtimestamp)<1000:
            self.prevtimestamp.append(time)
        else:
            self.prevtimestamp=[]
            self.prevtimestamp.append(time)

    def processCBT(self, cbt):
        if cbt.action=='ONLINE_PEERLIST':
            interface_name = cbt.data.get("interface_name")
            self.ipop_interface_details[interface_name]["peerlist"] = list(sorted(cbt.data['peerlist']))
            self.ipop_interface_details[interface_name]["mac"]      = cbt.data.get("mac")
        elif cbt.action=='BroadcastPkt':
                self.sendtopeer(cbt.data,"BroadcastPkt")
        elif cbt.action=='BroadcastData':
            self.sendtopeer(cbt.data,"BroadcastData")
        else:
            log = '{0}: unrecognized CBT message {1} received from {2}.Data:: {3}' \
                .format(cbt.recipient, cbt.action, cbt.initiator, cbt.data)
            self.registerCBT('Logger', 'warning', log)

    def sendtopeer(self, data,datype):
        interface_name = data.get("interface_name")
        if self.ipop_interface_details[interface_name]["peerlist"]:
            if data["type"] == "local":
                # Message originated at this node. Pass to all the Peers (with uid greater than itself).
                self.registerCBT('Logger', 'debug',"Broadcast message obtained from the local Tap interface")
                self.sendPktToAllPeers(sorted(self.ipop_interface_details[interface_name]["peerlist"]),\
                                       data["dataframe"],datype,data["interface_name"])
            else:
                messagetime = data["put_time"]
                if self.prevtimestamp.count(messagetime) == 0:
                    self.inserttimestamp(messagetime)
                    # Message originated at some other node. Pass to peers upto the incoming successor uid.
                    self.registerCBT('Logger', 'debug',"Broadcast message received from peer node.")
                    self.sendPkt(data["dataframe"], data["init_uid"], data["peer_list"], messagetime,datype,data["interface_name"])
                    # Passing the message to itself.
                    self.recvPkt(data,data["message_type"],data["interface_name"])
        else:
            self.registerCBT('Logger', 'info', "No online peers available for broadcast.")
            self.registerCBT('ConnectionManager', 'TINCAN_CONTROL', {"interface_name": data["interface_name"],
                                                                     "type": "get_online_peerlist"})

    def forwardMessage(self,msg_frame,init_id,suc_id,peer,peer_list,time,datype,interface_name):
         self.send_count+=1
         cbtdata = {
                        "msg_type": "forward",
                        "src_uid": suc_id,
                        "dst_uid": peer,
                        "interface_name": interface_name,
                        "msg": {
                                 "dataframe":str(msg_frame),
                                 "init_uid": init_id,
                                 "peer_list" : peer_list,
                                 "put_time" :  time,
                                 "message_type" : datype
                               }
                   }
         #self.registerCBT('Logger', 'debug', '@@@ Final Message : ' + str(cbtdata))
         self.registerCBT('BaseTopologyManager', 'ICC_CONTROL', cbtdata)

    # Method to forward message to peers by the Initiating node.
    def sendPktToAllPeers(self,plist,data,datype,interface_name):
          # Considering the node with the highest uid.
          self.registerCBT('Logger','info','Sending broadcast packet to all online peers'+str(plist))
          uid = self.ipop_interface_details[interface_name]["uid"]
          messageputtime = int(round(time.time()*1000))
          # Case when the initiator is the last node in the network
          if uid > max(plist):
                self.registerCBT('Logger','info','Broadcast message sent to peer: '+str(plist[0]))
                self.forwardMessage(data,uid,uid,plist[0],[plist[0],uid],messageputtime,datype,interface_name)
          else:
                for ind,peer in enumerate(plist):
                    #if self.uid < peer: #Message sent to all the nodes with a uid greater than the self (clockwise)
                        if ind==len(plist)-1:
                            suc_id = plist[0]
                        else:
                            suc_id = plist[ind+1]
                        # Appending the message with the next succesor and the initiator
                        self.registerCBT('Logger','debug','Broadcast message sent to Successor uid: {0}'.format(peer))
                        self.forwardMessage(data,uid,uid,plist[ind],[peer,suc_id],messageputtime,datype,interface_name)

    # Method to forward packets when the initiator is elsewhere
    def sendPkt(self,data_frame,init_id,in_plist,messagetime,datype,interface_name):
          self.registerCBT('Logger','info','Sending broadcast packet to suitable peers.')
          #self.registerCBT('Logger','info','@@@ Incoming suc uid : '+init_id)
          uid = self.ipop_interface_details[interface_name]["uid"]
          plist = sorted(self.ipop_interface_details[interface_name]["peerlist"])

          if uid >= max(in_plist) and uid > init_id:
              for peer in plist:
                    if peer != init_id and peer > uid:
                        self.registerCBT('Logger', 'debug', 'Broadcast message sent to UID: {0}'.format(peer))
                        self.forwardMessage(data_frame, init_id, uid, peer, in_plist, messagetime,datype,interface_name)

          elif uid <= min(in_plist) and uid < init_id:
              for peer in plist:
                  if init_id >= max(in_plist):
                      if uid < peer and in_plist.count(peer) == 0 and peer != init_id:
                          self.registerCBT('Logger', 'debug',
                                           'Broadcast message sent to UID: {0}'.format(peer))
                          self.forwardMessage(data_frame, init_id, uid, peer, in_plist, messagetime,datype,interface_name)
                  else:
                      if uid > peer and in_plist.count(peer) == 0 and peer != init_id:
                          self.registerCBT('Logger', 'debug',
                                           'Broadcast message sent to UID: {0}'.format(peer))
                          self.forwardMessage(data_frame, init_id, uid, peer, in_plist, messagetime,datype,interface_name)

          else:
              for peer in plist:
                  if uid < peer and in_plist.count(peer) == 0 and peer != init_id and peer < max(in_plist):
                      self.registerCBT('Logger', 'debug', 'Broadcast message sent to UID: {0}'.format(peer))
                      self.forwardMessage(data_frame, init_id, uid, peer, in_plist, messagetime,datype,interface_name)

    def recvPkt(self, data, messagetype,interface_name):
        #self.receive_count += 1
        #messagedetails = {"send": self.send_count, "receive": self.receive_count, "interface_name": interface_name}
        #self.registerCBT('BaseTopologyManager', 'Send_Receive_Details', messagedetails)
        if messagetype != "BroadcastData" and data["type"] == "remote":
            self.registerCBT('Logger', 'info', 'Going to insert Broadcast Packet to Tap interface')
            self.registerCBT('TincanInterface', 'DO_INSERT_DATA_PACKET', data)


    def terminate(self):
        pass

    def timer_method(self):
        pass

