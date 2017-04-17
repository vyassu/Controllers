#!/usr/bin/env python
import os,sys,json,signal
import controller.framework.fxlib as fxlib
import controller.framework.ipoplib as ipoplib
import argparse,threading,importlib
from getpass import getpass
from collections import OrderedDict
from controller.framework.CBT import CBT as _CBT
from controller.framework.CFxHandle import CFxHandle


class CFX(object):

    def __init__(self):
        self.CONFIG = {}
        self.parse_config()
        '''
        CFxHandleDict is a dict containing the references to CFxHandles of all CMs with key as the module name and
        value as the CFxHandle reference
        '''
        self.CFxHandleDict = {}
        self.vpn_type = self.CONFIG['CFx']['Model']

        
    def submitCBT(self, CBT):
        recipient = CBT.recipient
        self.CFxHandleDict[recipient].CMQueue.put(CBT)

    def createCBT(self, initiator='', recipient='', action='', data=''):
        # create and return an empty CBT
        cbt = _CBT(initiator, recipient, action, data)
        return cbt

    def freeCBT(self):
        # deallocate CBT (use python's automatic garbage collector)
        pass


    def initialize(self,):
        self.loaded_modules = ['CFx']  # list of modules already loaded

        # check for circular dependencies in the configuration file
        dependency_graph = {}
        print(self.CONFIG)
        for key in self.CONFIG:
            if(key != 'CFx'):
                try:
                    dependency_graph[key] = self.CONFIG[key]['dependencies']
                except:
                    pass

        if self.detect_cyclic_dependency(dependency_graph):
            print("Circular dependency detected in config.json. Exiting")
            sys.exit()

        # iterate and load the modules specified in the configuration file
        for key in self.CONFIG:
            if key not in self.loaded_modules:
                self.load_module(key)

        # start all the worker and timer threads
        for handle in self.CFxHandleDict:
            self.CFxHandleDict[handle].CMThread.start()
            if self.CFxHandleDict[handle].timer_thread:
                self.CFxHandleDict[handle].timer_thread.start()

    def load_module(self, module_name):
        if 'Enabled' in self.CONFIG[module_name]:
            module_enabled = self.CONFIG[module_name]['Enabled']
        else:
            module_enabled = True

        if (module_name not in self.loaded_modules) and module_enabled and module_name != "Tincan":
            # load the dependencies of the module
            self.load_dependencies(module_name)

            # import the modules dynamically
            try:
                module = importlib.import_module("controller.modules.{0}".format(module_name))
            except ImportError:
                if self.vpn_type == "GroupVPN":
                    module = importlib.import_module("controller.modules.gvpn.{0}".format(module_name))
                elif self.vpn_type == "SocialVPN":
                    module = importlib.import_module("controller.modules.svpn.{0}".format(module_name))
                else:
                    module = importlib.import_module("controller.modules.{0}.{1}".format(self.vpn_type, module_name))

            # get the class with name key from module
            module_class = getattr(module, module_name)

            # create a CFxHandle object for each module
            handle = CFxHandle(self)

            # instantiate the class with the CFxHandle reference and the
            # configuration parameter (additionally, pass the list of sockets to
            # the TincanListener and TincanSender modules
            '''
            if module_name in ['TincanListener']:
                instance = module_class([self.sock, self.sock_svr],
                                        handle,
                                        self.CONFIG[module_name],
                                        module_name)
            elif module_name in ['TincanSender']:
                instance = module_class([self.sock],
                                        handle,
                                        self.CONFIG[module_name],
                                        module_name)
            else:
            '''
            instance = module_class(handle, self.CONFIG[module_name], module_name)

            handle.CMInstance = instance
            handle.CMConfig = self.CONFIG[module_name]

            # store the CFxHandle object references in the
            # dict with module name as the key
            self.CFxHandleDict[module_name] = handle

            # intialize all the CFxHandles which in turn initialize the CMs
            handle.initialize()

            self.loaded_modules.append(module_name)

    def load_dependencies(self, module_name):
        # load the dependencies of the module as specified in the configuration file
        try:
            dependencies = self.CONFIG[module_name]['dependencies']
            for module in dependencies:
                if module not in self.loaded_modules:
                    self.load_module(module)
        except KeyError:
            pass

    def detect_cyclic_dependency(self, g):
        # test if the directed graph g has a cycle
        path = set()
        def visit(vertex):
            path.add(vertex)
            for neighbour in g.get(vertex, ()):
                if (neighbour in path) or visit(neighbour):
                    return True
            path.remove(vertex)
            return False

        return any(visit(v) for v in g)

    def __handler(self, signum=None, frame=None):
        print('Signal handler called with signal ', signum)

    def parse_config(self):
        self.CONFIG = fxlib.CONFIG

        parser = argparse.ArgumentParser()
        parser.add_argument("-c", help="load configuration from a file",
                            dest="config_file", metavar="config_file")
        parser.add_argument("-u", help="update configuration file if needed",
                            dest="update_config", action="store_true")
        parser.add_argument("-p", help="load remote ip configuration file",
                            dest="ip_config", metavar="ip_config")
        parser.add_argument("-s", help="configuration as json string"
                            " (overrides configuration from file)",
                            dest="config_string", metavar="config_string")
        parser.add_argument("--pwdstdout", help="use stdout as "
                            "password stream",
                            dest="pwdstdout", action="store_true")

        args = parser.parse_args()

        if args.config_file:
            # load the configuration file
            with open(args.config_file) as f:
                # load the configuration file into an OrderedDict with the
                # modules in the order in which they appear
                self.json_data = json.load(f, object_pairs_hook=OrderedDict)
                for key in self.json_data:
                    if self.CONFIG.get(key, False):
                        self.CONFIG[key].update(self.json_data[key])
                    else:
                        self.CONFIG[key] = self.json_data[key]

        if args.config_string:
            loaded_config = json.loads(args.config_string)
            for key in loaded_config:
                if self.CONFIG.get(key, None):
                    self.CONFIG[key].update(loaded_config[key])

        need_save = self.setup_config(self.CONFIG)
        if need_save and args.config_file and args.update_config:
            with open(args.config_file, "w") as f:
                json.dump(self.CONFIG, f, indent=4, sort_keys=True)
        '''
        if not ("Username" in self.CONFIG["XmppClient"] and
                        "AddressHost" in self.CONFIG["XmppClient"]):
            raise ValueError("At least XMPP 'Username' and 'AddressHost' "
                             "must be specified in config file or string")
        keyring_installed = False
        try:
            import keyring
            keyring_installed = True
        except:
            print("Key-ring module is not installed")

        if "Password" not in self.CONFIG["XmppClient"]:
            xmpp_pswd = None
            if keyring_installed:
                xmpp_pswd = keyring.get_password("ipop",
                                                 self.CONFIG["XmppClient"]["Username"])
            if not keyring_installed or (keyring_installed and xmpp_pswd == None):
                prompt = "\nPassword for %s:" % self.CONFIG["XmppClient"]["Username"]
                if args.pwdstdout:
                    xmpp_pswd = getpass(prompt, stream=sys.stdout)
                else:
                    xmpp_pswd = getpass(prompt)

            if xmpp_pswd != None:
                self.CONFIG["XmppClient"]["Password"] = xmpp_pswd
                if keyring_installed:
                    try:
                        keyring.set_password("ipop", self.CONFIG["XmppClient"]["Username"],
                                             self.CONFIG["XmppClient"]["Password"])
                    except:
                        print("unable to store password in keyring")
            else:
                raise RuntimeError("no XMPP password found")
        '''
        if args.ip_config:
            fxlib.load_peer_ip_config(args.ip_config)

    def setup_config(self, config):
        # validate config; return true if the config is modified
        if not config['CFx']['local_uid']:
            uid = ipoplib.uid_b2a(os.urandom(self.CONFIG['CFx']['uid_size'] // 2))
            self.CONFIG['CFx']["local_uid"] = uid
            return True  # modified
        return False

    def waitForShutdownEvent(self):
        self.event = threading.Event()

        # Since signal.pause() is not avaialble on windows, use event.wait()
        # with a timeout to catch KeyboardInterrupt. Without timeout, it's
        # not possible to catch KeyboardInterrupt because event.wait() is
        # a blocking call without timeout. The if condition checks if the os
        # is windows.
        if os.name == 'nt':
            while True:
                try:
                    self.event.wait(1)
                except (KeyboardInterrupt, SystemExit) as e:
                    break
        else:
            for sig in [signal.SIGINT]:
                signal.signal(sig, self.__handler)

            # signal.pause() sleeps until SIGINT is received
            signal.pause()

    def terminate(self):
        for key in self.CFxHandleDict:
            # create a special terminate CBT to terminate all the CMs
            terminateCBT = self.createCBT('CFx', key, 'TERMINATE', '')

            # clear all the queues and put the terminate CBT in all the queues
            self.CFxHandleDict[key].CMQueue.queue.clear()

            self.submitCBT(terminateCBT)

        # wait for the threads to process their current CBTs and exit
            print("waiting for timer threads to exit gracefully...")
        for handle in self.CFxHandleDict:
            if self.CFxHandleDict[handle].joinEnabled:
                self.CFxHandleDict[handle].CMThread.join()
                if self.CFxHandleDict[handle].timer_thread:
                    self.CFxHandleDict[handle].timer_thread.join()

        sys.exit(0)

    def queryParam(self, ModuleName,ParamName=""):
        try:
            if ModuleName in [None,""]:
                return None
            else:
                if ParamName == "":
                    return None
                else:
                    return self.CONFIG[ModuleName][ParamName]
        except Exception as error:
            print("Exception occurred while querying data."+str(error.message))
            return None

if __name__=="__main__":
    cf = CFX()
    cf.initialize()
