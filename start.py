import logging
import os

from thespian.actors import ActorSystem
from thor.utils import ServerMode

from thor.actors import DirectoryServer
from raft.server import Server
from raft.cluster import Cluster

KEY_SPACE = 1000
CLUSTER_SIZE = 3
REPLICAS = 15
CLUSTER_COUNT = int(REPLICAS/CLUSTER_SIZE)

logcfg = {
    'version': 1,
    'formatters': {
        'normal': {
            'format': '%(asctime)-8s %(message)s'
        }
    },
    'handlers': {
        'h': {
            'class': 'logging.FileHandler',
            'filename': 'thor.log',
            'formatter': 'normal',
            'level': logging.DEBUG
        }
    },
    'loggers': {
        '': {
            'handlers': ['h'],
            'level': logging.DEBUG
        }
    }
}


def _group_by_cluster(cluster_map: dict) -> dict:
    answer = {}
    for oid, cluster in cluster_map.items():
        if cluster not in answer:
            answer[cluster] = set()
        answer[cluster].add(oid)
    return answer


def start(sys_base):
    asys = ActorSystem(sys_base, logDefs=logcfg)
    ds = asys.createActor(DirectoryServer, globalName="directory-server")
    db_servers = {}
    for i in range(0, REPLICAS):
        name = "db-server-%d" % i
        db_servers[name] = asys.createActor(Server, globalName=name)

    clusters = []
    server_list = list(db_servers.keys())
    for i in range(0, CLUSTER_COUNT):
        cluster = Cluster("cluster-{}".format(i), server_list[i*CLUSTER_SIZE : i*CLUSTER_SIZE + CLUSTER_SIZE])
        clusters.append(cluster)

    cluster_map = {}
    for key in range(1, KEY_SPACE + 1):
        cluster_map[key] = clusters[key % CLUSTER_COUNT]

    asys.ask(ds, DirectoryServer.WhoServes(cluster_map))

    grouped_cluster_map = _group_by_cluster(cluster_map)

    for cluster, oid_set in grouped_cluster_map.items():
        for server_name in cluster.members:
            asys.ask(db_servers[server_name], Server.View(oid_set, cluster, "directory-server"))


def start_thor(sys_base):
    asys = ActorSystem(sys_base, logDefs=logcfg)
    # ds = asys.createActor(DirectoryServer, globalName="directory-server")
    db_servers = {}
    for i in range(0, 3):
        name = "server-%d" % i
        db_servers[name] = asys.createActor(Server, globalName=name)
    
    for server in db_servers.values():
        asys.ask(server, Server.View({}, list(db_servers.values())))


if __name__ == "__main__":
    try:
        os.remove("thor.log")
    except:
        pass
    start("multiprocTCPBase")
    # start("simpleSystemBase")
