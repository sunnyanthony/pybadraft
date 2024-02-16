from abc import ABC, abstractmethod
from src.roles import NodeState

class RaftNodeBase(ABC):
    def __init__(self, id, port, peers):
        self.id = id
        self.port = port
        self.peers = peers
        self.state = NodeState.FOLLOWER
        self.term = 0
        self.voted_for = None
        self.votes_received = 0

    @classmethod
    def create_raft(type="threading"):
        if type == "threading":
            from src.raft_threading import RaftNode
            return RaftNode
        else:
            from src.raft_async import AsyncRaftNode
            return AsyncRaftNode

    @abstractmethod
    async def send_vote_request(self, peer):
        pass

    @abstractmethod
    async def request_votes(self):
        pass

    @abstractmethod
    async def start_server(self):
        pass

    @abstractmethod
    async def run(self):
        pass
