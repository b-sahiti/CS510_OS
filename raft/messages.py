class Heartbeat:
    def __init__(self, term):
        self.term = term

class TimeToHeartbeat:
    pass

class LeaderTimeout:
    pass

class RequestVote:
    def __init__(self, term, candidate, log_len):
        self.term = term
        self.candidate = candidate
        self.log_len = log_len

class Vote:
    pass


class UpdateYourTerm:
    pass

class LeaderChange:
    def __init__(self, cluster_name, leader_name):
        self.cluster_name = cluster_name
        self.leader_name = leader_name

class AppendLog:
    def __init__(self, trx, serial):
        self.trx = trx
        self.serial = serial

class RepairLogReq:
    def __init__(self, offset, requester):
        self.offset = offset
        self.requester = requester

class RepairLog:
    def __init__(self, entries):
        self.entries = entries
    
class Recover:
    pass