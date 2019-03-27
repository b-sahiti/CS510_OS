import logging
import time
import random

from sortedcontainers import SortedListWithKey
from thespian.actors import Actor, WakeupMessage

from thespian.actors import ActorExitRequest, ActorSystem

from thor.actors import DirectoryServer
from thor.utils import Transaction, ServerMode
from enum import Enum
import raft.messages as MSG
import raft.config as config
from random import randint
from raft.cluster import Cluster

class Role(Enum):
    LEADER = 1
    CANDIDATE = 2
    FOLLOWER = 3


class Server(Actor):
    THRESHOLD = 2
    HEARTBEAT_INTERVAL = 1

    class View:
        def __init__(self, keys: set, cluster: Cluster, coordinator: str):
            self.keys = keys
            self.cluster = cluster
            self.coordinator = coordinator

    class Objects:
        def __init__(self, transaction: Transaction):
            self.transaction = transaction

    class Prepare:
        def __init__(self, transaction: Transaction):
            self.transaction = transaction

    class Commit:
        def __init__(self, transaction: Transaction):
            self.transaction = transaction

    class Abort:
        def __init__(self, transaction: Transaction):
            self.transaction = transaction

    class CacheInvalidate:
        def __init__(self, oid):
            self.oid = oid

    class TruncateHistory:
        pass

    def __init__(self, globalName=None):
        self.votes_rcvd = 1
        self.role = Role.FOLLOWER
        self.store = {}
        self.cache_table = {}
        self.cluster = None
        self.coordinator = None
        self.term = 0
        self.LEADERTIMEOUT = randint(5, 20)
        self.received_heartbeat = False
        self.log_serial = 1
        self.log = {}
        self.history = SortedListWithKey(key=lambda t: t.timestamp)
        self.up = True
        super().__init__(globalName=globalName)

    def receiveMessage_leader(self, msg, sender):
        if isinstance(msg, Server.Prepare):
            self.send(sender, self.prepare_trx(msg))

        elif isinstance(msg, Server.Commit):
            self.send(sender, self.commit_trx(msg))

        elif isinstance(msg, Server.Abort):
            self.send(sender, self.abort_trx(msg))
            
        elif isinstance(msg, WakeupMessage):
            if isinstance(msg.payload, Server.TruncateHistory):
                self.truncate_history()
                self.wakeupAfter(Server.THRESHOLD, Server.TruncateHistory())
            elif isinstance(msg.payload, MSG.TimeToHeartbeat):
                for member in self.cluster.members:
                    self.send(self.createActor(Server, globalName=member), MSG.Heartbeat(self.term))
                self.wakeupAfter(Server.HEARTBEAT_INTERVAL, MSG.TimeToHeartbeat())

        elif isinstance(msg, MSG.RequestVote):
            if msg.term > self.term:
                self.role = Role.FOLLOWER
                self.send(sender, MSG.Vote())

        elif isinstance(msg, MSG.RepairLogReq):
            resp_entries = {k: self.log[k] for k in self.log if k > msg.offset}
            logging.debug("%s: received repair log request from %s with %d offset. Sending %d entries", self.globalName, msg.requester, msg.offset, len(resp_entries))
            self.send(sender, MSG.RepairLog(resp_entries))

    def receiveMessage_candidate(self, msg, sender):
        if isinstance(msg, MSG.Vote):
            logging.debug("%s: Received vote for term %d", self.globalName, self.term + 1)
            self.votes_rcvd += 1
            if self.votes_rcvd > config.CLUSTER_SIZE/2:
                self.term += 1
                self.role = Role.LEADER
                logging.debug("%s: Became leader for term %d", self.globalName, self.term)
                for member in self.cluster.members:
                    self.send(self.createActor(Server, globalName=member), MSG.Heartbeat(self.term))
                self.send(self.coordinator, MSG.LeaderChange(self.cluster.name, self.globalName))
                self.wakeupAfter(Server.HEARTBEAT_INTERVAL, MSG.TimeToHeartbeat())
            
        elif isinstance(msg, MSG.RequestVote):
            if msg.term > self.term + 1:
                self.role = Role.FOLLOWER
                self.send(sender, MSG.Vote())

    def receiveMessage_follower(self, msg, sender):
        if isinstance(msg, MSG.Heartbeat):
            if msg.term > self.term:
                self.term = msg.term
                logging.debug("%s: Found new leader for term %d", self.globalName, self.term)
            logging.debug("%s: Received heartbeat %d", self.globalName, self.term)
            self.received_heartbeat = True

        elif isinstance(msg, MSG.AppendLog):
            if msg.serial > len(self.log) + 1:
                logging.debug("%s: received trx with serial %d. Only have %d trx in log", self.globalName, msg.serial, len(self.log))
                self.send(sender, MSG.RepairLogReq(len(self.log), self.globalName))
            elif msg.serial == len(self.log) + 1:
                for oid, val in msg.trx.write_set.items():
                    store_version = self.store[oid][1]
                    self.store[oid] = (val, store_version + 1, msg.trx.timestamp)
                logging.debug("%s: appended trx %s, serial number %d as follower", self.globalName, msg.trx.tid, msg.serial)
                self.log[msg.serial] = msg.trx

        elif isinstance(msg, MSG.RepairLog):
            logging.debug("%s: received %d entries to repair log", self.globalName, len(msg.entries))
            self.log.update(msg.entries)

        elif isinstance(msg, MSG.RequestVote):
            if msg.term > self.term and msg.log_len >= len(self.log):
                logging.debug("%s: Voting for %s, term %d", self.globalName, msg.candidate, msg.term)
                self.send(sender, MSG.Vote())

        elif isinstance(msg, WakeupMessage):
            if isinstance(msg.payload, MSG.LeaderTimeout):
                if self.received_heartbeat:
                    self.received_heartbeat = False
                    self.wakeupAfter(self.LEADERTIMEOUT, MSG.LeaderTimeout())
                else:
                    logging.debug("%s: Role change, became CANDIDATE", self.globalName)
                    self.role = Role.CANDIDATE
                    for member in self.cluster.members:
                        logging.debug("%s: Sending RequestVote", self.globalName)
                        self.send(self.createActor(Server, globalName=member), MSG.RequestVote(self.term + 1, self.globalName, len(self.log)))


    def receiveMessage(self, msg, sender):
        if isinstance(msg, WakeupMessage) and isinstance(msg.payload, MSG.Recover):
            self.up = True
            logging.debug("%s: recovered", self.globalName)
        if not self.up:
            return


        if isinstance(msg, Server.Objects):
            # logging.debug("%s received read object request for %s", self.globalName, msg.transaction.read_set.keys())
            self.send(sender, self.get_objects(msg))
        elif isinstance(msg, Server.View):
            logging.debug("%s: View, my timeout is %d", self.globalName, self.LEADERTIMEOUT)
            # self.wakeupAfter(Server.THRESHOLD, Server.TruncateHistory())
            self.store = {k: (0, 0, 0) for k in msg.keys}
            self.cluster = msg.cluster
                
            self.coordinator = self.createActor(DirectoryServer, globalName=msg.coordinator)
            self.wakeupAfter(self.LEADERTIMEOUT, MSG.LeaderTimeout())
            self.send(sender, True)
            if self.globalName == "db-server-0":
                logging.debug("%s: going down", self.globalName)
                self.up = False
                self.wakeupAfter(26, MSG.Recover())


        elif self.role == Role.CANDIDATE:
            self.receiveMessage_candidate(msg, sender)
        elif self.role == Role.LEADER:
            self.receiveMessage_leader(msg, sender)
        elif self.role == Role.FOLLOWER:
            self.receiveMessage_follower(msg, sender)

    def get_objects(self, msg):
        answer = {}
        for oid in msg.transaction.read_set.keys():
            answer[oid] = self.store[oid][:-1]
            if oid not in self.cache_table:
                self.cache_table[oid] = []
            self.cache_table[oid].append(msg.transaction.clerk)
        return answer

    def truncate_history(self):
        threshold = time.time() - Server.THRESHOLD
        to_delete = self.history[:self.history.bisect_key_left(threshold)]
        # logging.debug("%s -Truncating history, deleting %d items",
                    #   self.globalName, len(to_delete))
        for item in to_delete:
            self.history.remove(item)

    def prepare_trx(self, msg: Prepare) -> bool:
        # self.truncate_history()
        trx = msg.transaction
        position = self.history.bisect_key_left(trx.timestamp)
        earlier_trxs, later_trxs = self.history[:position], \
                                   self.history[position:]

        # validate against earlier transactions
        for earlier_t in earlier_trxs:
            read_set_oids = set(trx.read_set.keys())
            if earlier_t.write_set.keys() & read_set_oids:
                logging.debug(
                    "%s -Validation failed for %s - Against earlier transactions - Prepare NO",
                    self.globalName, trx.tid)
                return False

        # validate against r_stamp
        for oid, shadow in trx.read_set.items():
            store_version = self.store[oid][1]
            r_stamp = self.store[oid][2]
            # if shadow[1] != store_version:
            if trx.timestamp < r_stamp or shadow[1] != store_version:
                logging.debug("%s -conflict, %s, %s", self.globalName, shadow,
                              self.store[oid])
                # logging.debug("%s -Validation failed for %s - transaction timestamp < rstamp - Prepare NO", self.globalName, trx.tid)
                return False

        # validate against later transactions
        for later_t in later_trxs:
            later_t_read_set_oids = set(later_t.read_set.keys())
            trx_read_set_oids = set(trx.read_set.keys())

            if trx.write_set.keys() & later_t_read_set_oids or \
               trx_read_set_oids & later_t.write_set.keys():
                logging.debug(
                    "%s -Validation failed for %s - Against later transactions - Prepare NO",
                    self.globalName, trx.tid)
                return False

        trx.status = "P"
        self.history.add(trx)
        logging.debug("%s Validation successful for %s - Prepare OK",
                      self.globalName, trx.tid)
        return True

    def commit_trx(self, msg: Commit) -> bool:
        trx = msg.transaction
        logging.debug("%s -Committing %s with \nread_set: %s\nwrite_set: %s",
                      self.globalName, trx.tid, trx.read_set.keys(),
                      trx.write_set.keys())
        try:
            index = self.history.index(trx)
            if self.history[index].status != "P":
                logging.debug("Transaction failed to commit")
                return False
        except ValueError:
            return False

        for oid, _ in trx.read_set.items():
            val = self.store[oid]
            self.store[oid] = (val[0], val[1], trx.timestamp)

        for oid, val in trx.write_set.items():
            store_version = self.store[oid][1]
            self.store[oid] = (val, store_version + 1, trx.timestamp)
            for clerk in self.cache_table.get(oid, []):
                logging.debug("%s -Sending cache invalidation for %d",
                              self.globalName, oid)
                self.send(clerk, Server.CacheInvalidate(oid))
            self.cache_table[oid].clear()

        trx.status = "C"
        self.history.remove(trx)
        entry = trx
        self.log[self.log_serial] = entry
        logging.debug("%s: commited trx with serial %d. Log length is now %d", self.globalName, self.log_serial, len(self.log))
        self.log_serial += 1
        for member in self.cluster.members:
            self.send(self.createActor(Server, globalName=member), MSG.AppendLog(entry, len(self.log)))
        return True

    def abort_trx(self, msg: Abort) -> bool:
        logging.debug("%s - Aborting transaction %s", self.globalName,
                      msg.transaction.tid)
        self.history.remove(msg.transaction)
        return True
