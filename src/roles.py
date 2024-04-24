import datetime
from typing import Optional, Protocol

from .packet import MetaData
from .raft import RaftNodeBase
from .packet import Request

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
        for data in getattr(node, "cmd_temp", []):
            node.toleader(data)

    def handle_request(self, node: RaftNodeBase, request: Optional[MetaData] = None) -> None:
        if request:
            data: MetaData = request
            if data.type == Request.COMMAND:
                node.appendlog(data)
            elif data.type == Request.USERCOMMAND:
                node.toleader(data)
            else:
                node.leader = data.id
                node.reset_election_timer()

class CandidateState(NodeState):
    def on_enter_state(self, node: RaftNodeBase) -> None:
        node.leader = -1
        node.reset_election_timer()

    def handle_request(self, node: RaftNodeBase, request: Optional[MetaData] = None) -> None:
        if request:
            data: MetaData = request
            if data.type == Request.USERCOMMAND:
                cmd_temp = getattr(self, "cmd_temp", [])
                cmd_temp.append(data)
                self.cmd_temp = cmd_temp
            elif data.type == Request.COMMAND:
                node.appendlog(data)

    def on_exit_state(self, node: RaftNodeBase) -> None:
        cmd_temp = getattr(self, "cmd_temp", [])
        if cmd_temp:
            node.cmd_temp = cmd_temp

class LeaderState(NodeState):
    def on_enter_state(self, node: RaftNodeBase) -> None:
        node.start_heartbeat()
        node.leader = node.id
        node.stop_election_timer()

    def on_exit_state(self, node: RaftNodeBase) -> None:
        # if node.heartbeat_timer:
        #     node.heartbeat_timer.cancel()
        node.heartbeat_timer = None

    def handle_request(self, node: RaftNodeBase, request: Optional[MetaData] = None) -> None:
        if request:
            data: MetaData = request
            if data.type == Request.COMMAND or data.type == Request.USERCOMMAND:
                data.type = Request.COMMAND
                node.appendlog(data)
                node.sendoutcmd(data)
