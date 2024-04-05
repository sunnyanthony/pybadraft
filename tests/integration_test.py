import pytest
import threading
import time
from src.raft_threading import RaftNode
from src.roles import NodeState
from functools import partial

@pytest.fixture(scope="module")
def setup_nodes():
    peers = [("127.0.0.1", 5001), ("127.0.0.1", 5002), ("127.0.0.1", 5003)]
    nodes = [
        RaftNode(id=1, port=5001, peers=peers[1:]),
        RaftNode(id=2, port=5002, peers=peers[:1] + peers[2:]),
        RaftNode(id=3, port=5003, peers=peers[:-1])
    ]
    threads = []
    for node in nodes:
        t = threading.Thread(target=partial(node.run, blocking=True))
        t.start()
        threads.append(t)
    yield nodes
    #for node in nodes:
    #    node.stop()

def test_raft_integration(setup_nodes):
    nodes = setup_nodes

    time.sleep(10)

    nodes[1].stop()

    time.sleep(10)

    leader_nodes = [node for node in nodes if node.state == NodeState.LEADER]
    assert len(leader_nodes) < 2, "More than one leader"
    assert len(leader_nodes) > 1, "No leader"
