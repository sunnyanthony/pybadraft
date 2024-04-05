import pytest
import threading
import time
from src.packet import MetaData, Request, load_packet, vote
from src.raft_threading import RaftNode
from src.roles import NodeState
from functools import partial

@pytest.fixture(scope="module")
def setup_nodes(request):
    peers = [("127.0.0.1", 5011), ("127.0.0.1", 5012), ("127.0.0.1", 5013)]
    nodes = [
        RaftNode(id=1, port=5011, peers=peers[1:]),
        RaftNode(id=2, port=5012, peers=peers[:1] + peers[2:]),
        RaftNode(id=3, port=5013, peers=peers[:-1])
    ]
    threads = []
    for node in nodes:
        t = threading.Thread(target=partial(node.run, blocking=True))
        t.start()
        threads.append(t)
    yield nodes
    for node in nodes:
        node.stop()

def test_data_packet():

    data = vote(True, 10, 234)
    assert load_packet(data) == MetaData(type=Request.VOTE_GRANTED, granted=True, term=10, id=234)

def test_raft_master_down_integration(setup_nodes):
    nodes = setup_nodes
    nodes[1].election_timer = threading.Timer(0.05, nodes[1].start_election)

    time.sleep(2)
    nodes[1].stop()
    time.sleep(2)

    leader_nodes = [node for node in nodes if node.state == NodeState.LEADER]
    assert len(leader_nodes) < 2, "More than one leader"
    assert len(leader_nodes) >= 1, "No leader"
