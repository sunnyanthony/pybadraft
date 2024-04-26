from abc import ABC, abstractmethod
import asyncio
import asyncio.queues
import datetime
import random
import threading
from typing import Any, List, Optional, Tuple, Union

from .packet import MetaData

class RaftNodeBase(ABC):
    def __init__(self, id: int, port: int, peers: List[Tuple[str, int]]):
        from .roles import NodeState, FollowerState
        self.id: int = id
        self.port: int = port
        self.peers: List[Tuple[str, int]] = peers
        self.peers_status: List[int] = [0] * len(peers)
        self._state: NodeState = FollowerState()
        self.term: int = 0
        self.leader: Tuple[int, str] = (-1, "")
        self._voted_for: Optional[int] = None
        self._voted_for_timeout: float = datetime.datetime.now().timestamp()
        self.votes_received: int = 0
        self.server_timeout: int = 3
        self.election_timeout: float = random.randint(150, 300) / 1000.0
        self.heartbeat_interval: float = 50 / 1000
        # voting timeout should be less than electino timeout
        self.voting_timeout: float = random.randint(100, int(self.election_timeout * 1000)) / 1000.0
        self.server: Optional[Any] = None
        self.lock: Union[threading.RLock, asyncio.Lock] = None
        self.threads: List[threading.Thread] = []
        self.stop_signal: Union[threading.Event, asyncio.Event] = None
        self.heartbeat_timer: Optional[threading.Thread] = None
        self.election_timer: Optional[threading.Thread] = None

    @classmethod
    def create_raft(type="threading"):
        if type == "threading":
            from src.raft_threading import RaftNode
            return RaftNode
        else:
            from src.raft_async import AsyncRaftNode
            return AsyncRaftNode

    @property
    @abstractmethod
    def voted_for(self) -> Any: ...

    @property
    @abstractmethod
    def state(self) -> Any: ...

    @abstractmethod
    def toleader(self, data:MetaData) -> None: ...

    @abstractmethod
    def appendlog(self, data:MetaData) -> None: ...

    @abstractmethod
    def sendoutcmd(self, data:MetaData) -> None: ...
    ## State Check and Transition

    @abstractmethod
    def check_votes(self) -> None: ...

    @abstractmethod
    def reset_state_on_heartbeat(self, data: MetaData) -> None: ...

    ## Server Handling

    @abstractmethod
    def handle_vote_request(self, data: MetaData, writer: Any) -> None: ...

    @abstractmethod
    def handle_client(self, reader: Any, writer: Any) -> None: ...

    @abstractmethod
    def start_server(self) -> None: ...

    ## Heartbeat
    @abstractmethod
    def send_heartbeat(self) -> None: ...

    @abstractmethod
    def start_heartbeat(self) -> None: ...

    ## Election

    @abstractmethod
    def request_votes(self) -> None: ...

    @abstractmethod
    def send_vote_request(self, peer: Tuple[str, int]) -> None: ...

    @abstractmethod
    def start_election(self) -> None:...

    @abstractmethod
    def reset_election_timer(self) -> None: ...

    @abstractmethod
    def stop_election_timer(self) -> None: ...

    ## Start
    @abstractmethod
    def run(self, blocking: bool = False) -> None: ...

    ## Stop
    @abstractmethod
    def stop(self) -> None: ...