import pytest
import threading
import time
from functools import partial

try:
    from src.packet import MetaData, Request, load_packet, vote
    from src.raft_threading import RaftNode
    from src.roles import LeaderState, FollowerState
except ModuleNotFoundError:
    from pybadraft.packet import MetaData, Request, load_packet, vote
    from pybadraft.raft_threading import RaftNode
    from pybadraft.roles import LeaderState, FollowerState

@pytest.fixture(scope="module")
def setup_nodes(request):
    peers = [("127.0.0.1", 5011), ("127.0.0.1", 5012), ("127.0.0.1", 5013)]
    nodes = [
        RaftNode(id=1, port=5011, peers=peers[1:], exposed=f"{peers[0][0]}:{peers[0][1]}"),
        RaftNode(id=2, port=5012, peers=peers[:1] + peers[2:], exposed=f"{peers[1][0]}:{peers[1][1]}"),
        RaftNode(id=3, port=5013, peers=peers[:-1], exposed=f"{peers[2][0]}:{peers[2][1]}")
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

    time.sleep(2)
    leader_nodes = [node for node in nodes if node.state == LeaderState]
    assert len(leader_nodes) == 1, "Initial election error"
    leader_nodes[0].stop()
    time.sleep(2)

    leader_nodes = [node for node in nodes if node.state == LeaderState]
    assert len(leader_nodes) < 2, "More than one leader"
    assert len(leader_nodes) >= 1, "No leader"

def test_raft_node_back_integration(setup_nodes):
    nodes = setup_nodes

    leader_nodes = [node for node in nodes if not node.threads]
    leader_nodes[0].run()
    time.sleep(2)

    leader_nodes = [node for node in nodes if node.state == LeaderState]
    assert len(leader_nodes) < 2, "More than one leader"
    assert len(leader_nodes) >= 1, "No leader"

    assert 2 == len([node for node in nodes if node.state == FollowerState])