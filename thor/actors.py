import logging
import time
import random

from sortedcontainers import SortedListWithKey
from thespian.actors import Actor, WakeupMessage

from .utils import Transaction, ServerMode
import raft.messages as MSG



class DirectoryServer(Actor):
    class RegisterListener:
        pass

    class GetTimestamp:
        def __init__(self, oids: set):
            self.oids = oids

    class WhoServes:
        def __init__(self, cluster_map: dict):
            self.cluster_map = cluster_map

    def __init__(self, globalName=None):
        self.cluster_map = {}
        self.leader_map = {}
        self.listeners = []
        super().__init__(globalName=globalName)

    def receiveMessage(self, msg, sender):
        if isinstance(msg, DirectoryServer.RegisterListener):
            #logging.debug("Register Listener for %s", self.globalName)
            self.listeners.append(sender)
            self.send(sender, DirectoryServer.WhoServes(self.get_leader_map()))
            
        elif isinstance(msg, DirectoryServer.WhoServes):
            logging.debug("%s: Who serves", self.globalName)
            self.cluster_map.update(msg.cluster_map)
            for listener in self.listeners:
                self.send(listener, DirectoryServer.WhoServes(self.cluster_map))
            self.send(sender, True)

        elif isinstance(msg, DirectoryServer.GetTimestamp):
            logging.debug("Getting simeStamp for %s", self.globalName)
            self.send(sender, (round(time.time() * 1000)))

        elif isinstance(msg, MSG.LeaderChange):
            self.leader_map[msg.cluster_name] = msg.leader_name
            logging.debug("%s: Leader changed for cluster %s=========", self.globalName, msg.cluster_name)
            for listener in self.listeners:
                self.send(listener, DirectoryServer.WhoServes(self.cluster_map))

    def get_leader_map(self):
        lm = {}
        for oid, cluster in self.cluster_map.items():
            lm[oid] = self.leader_map[cluster.name]
        return lm

    # def get_random_map(self):
    #     random_map = {}
    #     for oid, server_set in self.cluster_map.items():
    #         for _ in range(0, len(server_set)):
    #             server = random.choice(list(server_set.keys() - {"leader"}))
    #             if server_set[server] == ServerMode.NORMAL:
    #                 random_map[oid] = server
    #                 break
    #     return random_map
