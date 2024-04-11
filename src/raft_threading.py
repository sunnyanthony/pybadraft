from typing import List, Optional, Tuple
from functools import partial
from loguru import logger
import socket
import threading
import time
import random
from .decorator import synchronizer
from .roles import CandidateState, FollowerState, LeaderState, NodeState
from .packet import vote_request, vote, load_packet, heartbeat, Request, MetaData

class RaftNode:
    def __init__(self, id: int, port: int, peers: List[Tuple[str, int]], init_timeout: float=0) -> None:
        self.id: int = id
        self.port: int = port
        self.peers: List[Tuple[str, int]] = peers
        self.peers_status: List[int] = [0] * len(peers)
        self._state: NodeState = FollowerState()
        self.term: int = 0
        self.leader: int = -1
        self.voted_for: Optional[int] = None
        self.votes_received: int = 0
        self.server_timeout: int = 3
        self.election_timeout: float = random.randint(150, 300) / 1000.0
        self.lock: threading.RLock = threading.RLock()
        self.threads: List[threading.Thread] = []
        self.stop_signal: threading.Event = threading.Event()
        self.heartbeat_timer: Optional[threading.Timer] = None
        self.election_timer: threading.Timer = threading.Timer(self.election_timeout, self.start_election) if not init_timeout else threading.Timer(init_timeout, self.start_election)
        self._state.on_enter_state(self)
        logger.info(f"RaftNode {self.id} initialized with state: {self.state.__name__}, term: {self.term}")

    @property
    def state(self) -> type:
        return self._state.__class__

    @state.setter
    @synchronizer('lock')
    def state(self, value: type[NodeState]) -> None:
        if self._state.__class__ != value:
            self._state.on_exit_state(self)
            self._state = value()
            self._state.on_enter_state(self)
            logger.info(f"Node {self.id} state changed to {self._state.__class__.__name__}")

    @synchronizer('lock')
    def check_votes(self) -> None:
        if self.state == CandidateState and self.votes_received > len(self.peers) / 2:
            self.state = LeaderState
            logger.info(f"{self.id} is now the leader for term {self.term}.")

    def request_votes(self) -> None:
        threads = []
        for peer in self.peers:
            threads.append(threading.Thread(target=partial(self.send_vote_request, peer=peer)))
            threads[-1].start()
        
        for thread in threads:
            thread.join()

    def send_vote_request(self, peer: Tuple[str, int]) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.settimeout(self.election_timeout)
                s.connect(('localhost', peer[1]))
                s.sendall(vote_request(self.term, self.id))
                logger.debug(f"{self.id} sent vote request to {peer}")
                data = s.recv(1024)
                response_data = load_packet(data)
                if response_data.type == Request.VOTE_GRANTED and response_data.term == self.term and response_data.granted:
                    self.increment_votes()
            except ConnectionRefusedError:
                logger.error(f"{self.id} could not connect to {peer}")
            except socket.timeout:
                logger.error(f"Connection to {peer[0]} timed out")
            except Exception as e:
                logger.error(f"An error occurred while connecting or communicating with {peer[0]}: {str(e)}")

    @synchronizer('lock')
    def increment_votes(self) -> None:
        self.votes_received += 1
        if self.votes_received > len(self.peers) / 2:
            logger.info(f"{self.id} has received majority votes")
        self.check_votes()

    @synchronizer('lock')
    def handle_vote_request(self, data: MetaData, conn: socket.socket) -> None:
        term = data.term
        candidate_id = data.id
        vote_granted = False
        if (self.term < term) and (self.voted_for is None or self.voted_for == candidate_id):
            self.term = term
            self.voted_for = candidate_id
            self.state = FollowerState
            vote_granted = True
            self.reset_election_timer()
            logger.info(f"Vote granted by {self.id} to {candidate_id} for term {term}")
        else:
            logger.info(f"Vote denied by {self.id} to {candidate_id} for term {term}")
            logger.debug(f"Because of {self.term}, {self.voted_for}")
        data = vote(vote_granted, self.term, self.id)
        conn.sendall(data)

    def handle_connection(self, client_socket: socket.socket) -> None:
        try:
            while not self.stop_signal.is_set():
                timeout = random.randint(6, 10)
                client_socket.settimeout(timeout)
                data = client_socket.recv(1024)
                if not data:
                    continue
                metadata = load_packet(data)
                if metadata.type == Request.VOTE_REQUEST:
                    self.handle_vote_request(metadata, client_socket)
                elif metadata.type == Request.HEARTBEAT:
                    self.reset_state_on_heartbeat(metadata)
        except socket.timeout:
            logger.warning("Connection timeout")
        except ConnectionResetError:
            logger.warning("Connection reset by peer")
        finally:
            client_socket.close()

    @synchronizer('lock')
    def reset_state_on_heartbeat(self, data: MetaData) -> None:
        if data.term >= self.term:
            self.term = data.term
            if self.state == CandidateState:
                self.state = FollowerState
                logger.info(f"Received heartbeat, reset state to FOLLOWER for {self.id}")
            self._state.handle_request(self, data)

    def start_server(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', self.port))
            s.listen()
            s.settimeout(self.server_timeout)
            print_leader = 0
            logger.info(f"Server started on port {self.port}")
            while not self.stop_signal.is_set():
                if print_leader % 10 == 0:
                    print_leader = 0
                    logger.debug(f"Node {self.id}: leader now is {self.leader}")
                print_leader += 1
                try:
                    conn, addr = s.accept()
                    thread = threading.Thread(target=self.handle_connection, args=(conn,))
                    thread.start()
                    self.threads.append(thread)
                except socket.timeout:
                    continue

    def send_heartbeat(self) -> None:
        for idx, peer in enumerate(self.peers):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(('localhost', peer[1]))
                    s.settimeout(0.001)
                    s.sendall(heartbeat(self.id, self.term))
                self.peers_status[idx] = 0
            except ConnectionRefusedError:
                if self.peers_status[idx] == 0:
                    logger.error(f"Node {self.id} could not connect to {peer[1]}")
                    self.peers_status[idx] = 1
            except Exception as e:
                logger.error(f"Node {self.id}: {e}")

    def give_up_election(self) -> None:
        self.voted_for = None
        self.votes_received = 0

    def start_election(self) -> None:
        should_request_votes = False
        with self.lock:
            if self.stop_signal.is_set():
                return
            if self.state != LeaderState and (self.voted_for is None or self.voted_for == self.id):
                self.state = CandidateState
                self.term += 1
                should_request_votes = True
                self.voted_for = self.id
                self.votes_received = 1
                logger.info(f"{self.id} becomes a candidate and starts election for term {self.term}.")

        if should_request_votes:
            self.request_votes()

        timeout = self.election_timeout + random.randint(10, 100) / 1000.0
        threading.Timer(timeout/2, self.give_up_election).start()
        self.election_timer = threading.Timer(timeout, self.start_election)
        self.election_timer.start()

    def reset_election_timer(self) -> None:
        self.election_timer.cancel()
        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        self.election_timer.start()

    @synchronizer('lock')
    def state_check(self) -> None:
        if self.state == LeaderState:
            self.send_heartbeat()
            self._state.handle_request(self)
        return

    def start_heartbeat(self) -> None:
        self.heartbeat_timer = threading.Timer(50/1000, self.state_check)
        self.heartbeat_timer.start()

    def _run(self) -> None:
        self.stop_signal.clear()
        thread = threading.Thread(target=self.start_server)
        thread.start()

        self.threads.append(thread)
        while not self.stop_signal.is_set():
            time.sleep(1)

    def run(self, blocking: bool=False) -> None:
        logger.info(f"Node {self.id} starting...")
        thread = threading.Thread(target=self._run, daemon=True)
        self.threads.append(thread)
        thread.start()
        if blocking:
            for t in self.threads:
                t.join()
    
    def stop(self) -> None:
        logger.info(f"Node {self.id} stopping")
        self.election_timer.cancel()
        self.stop_signal.set()
        for thread in self.threads:
            thread.join()
        self.state = FollowerState
        logger.info(f"Node {self.id} stopped")
