import datetime
from typing import Optional, Protocol

from .packet import MetaData
from .raft import RaftNodeBase

class NodeState(Protocol):
    def on_enter_state(self, node: RaftNodeBase) -> None:
        ...

    def on_exit_state(self, node: RaftNodeBase) -> None:
        ...

    def handle_request(self, node: RaftNodeBase, request: Optional[MetaData] = None) -> None:
        ...

class FollowerState(NodeState):
    def on_enter_state(self, node: RaftNodeBase) -> None:
        node.reset_election_timer()

    def handle_request(self, node: RaftNodeBase, request: Optional[MetaData] = None) -> None:
        if request:
            data: MetaData = request
            node.leader = data.id
            node.reset_election_timer()

class CandidateState(NodeState):
    def on_enter_state(self, node: RaftNodeBase) -> None:
        node.leader = -1
        node.reset_election_timer()

class LeaderState(NodeState):
    def on_enter_state(self, node: RaftNodeBase) -> None:
        node.start_heartbeat()
        node.leader = node.id
        node.election_skip = datetime.datetime.now().timestamp() + 100
        election_timer = node.election_timer
        node.election_timer = None

    def on_exit_state(self, node: RaftNodeBase) -> None:
        # if node.heartbeat_timer:
        #     node.heartbeat_timer.cancel()
        node.heartbeat_timer = None

    def handle_request(self, node: RaftNodeBase, request: Optional[MetaData] = None) -> None:
        # Handle client requests, etc.
        node.start_heartbeat()
