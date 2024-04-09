from typing import Protocol

from .raft import RaftNodeBase

class NodeState(Protocol):
    def on_enter_state(self, node: RaftNodeBase):
        ...

    def on_exit_state(self, node: RaftNodeBase):
        ...

    def handle_request(self, node: RaftNodeBase, request = None):
        ...

class FollowerState(NodeState):
    def on_enter_state(self, node: RaftNodeBase):
        node.reset_election_timer()

    def handle_request(self, node: RaftNodeBase, request = None):
        node.reset_election_timer()

class CandidateState(NodeState):
    ...

class LeaderState(NodeState):
    def on_enter_state(self, node: RaftNodeBase):
        node.send_heartbeat()
        node.start_heartbeat()

    def on_exit_state(self, node: RaftNodeBase):
        node.heartbeat_timer.cancel()

    def handle_request(self, node: RaftNodeBase, request = None):
        # Handle client requests, etc.
        node.start_heartbeat()