from loguru import logger
import socket
import threading
import time
import random
from src.decorator import synchronizer
from src.roles import NodeState
from src.packet import vote_request, vote, load_packet, heartbeat, Request, MetaData

class RaftNode:
    def __init__(self, id, port, peers):
        self.id = id
        self.port = port
        self.peers = peers
        self.state = NodeState.FOLLOWER
        self.term = 0
        self.voted_for = None
        self.votes_received = 0
        self.server_timeout = 3
        self.lock = threading.RLock()
        self.threads = []
        self.stop_signal = threading.Event()
        logger.info(f"RaftNode {self.id} initialized with state: {self.state}, term: {self.term}")

    @synchronizer('lock')
    def become_leader(self):
        if self.state == NodeState.CANDIDATE and self.votes_received > len(self.peers) / 2:
            self.state = NodeState.LEADER
            logger.info(f"{self.id} is now the leader for term {self.term}.")

    @synchronizer('lock')
    def request_votes(self):
        self.state = NodeState.CANDIDATE
        self.term += 1
        self.voted_for = self.id
        self.votes_received = 1
        logger.info(f"{self.id} becomes a candidate and starts election for term {self.term}.")

    def send_vote_request(self, peer):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect(('localhost', peer[1]))
                s.sendall(vote_request(self.term, self.id))
                data = s.recv(1024)
                response_data = load_packet(data)
                if response_data.type == Request.VOTE_GRANTED:
                    self.increment_votes()
            except ConnectionRefusedError:
                logger.error(f"{self.id} could not connect to {peer[0]}")

    @synchronizer('lock')
    def increment_votes(self):
        self.votes_received += 1
        if self.votes_received > len(self.peers) / 2:
            logger.info(f"{self.id} has received majority votes")
        self.become_leader()

    @synchronizer('lock')
    def handle_vote_request(self, data: MetaData, conn):
        term = data.term
        candidate_id = data.id
        vote_granted = False
        if (self.term < term) and (self.voted_for is None or self.voted_for == candidate_id):
            self.term = term
            self.voted_for = candidate_id
            self.state = NodeState.FOLLOWER
            vote_granted = True
            logger.info(f"Vote granted by {self.id} to {candidate_id} for term {term}")
        else:
            logger.info(f"Vote denied by {self.id} to {candidate_id} for term {term}")
        conn.sendall(vote(vote_granted, self.term))

    def handle_connection(self, client_socket):
        try:
            while True:
                timeout = random.randint(6, 10)
                client_socket.settimeout(timeout)
                data = client_socket.recv(1024)
                if not data:
                    logger.warning("Connection terminated, no data received")
                    break
                metadata = load_packet(data)
                if metadata.type == Request.VOTE_REQUEST:
                    self.handle_vote_request(metadata, client_socket)
                elif metadata.type == Request.HEARTBEAT:
                    self.reset_state_on_heartbeat(metadata)
        except socket.timeout:
            logger.warning("Connection timeout")
        finally:
            client_socket.close()

    @synchronizer('lock')
    def reset_state_on_heartbeat(self, data: MetaData):
        if data.term >= self.term:
            self.term = data.term
            if self.state == NodeState.CANDIDATE:
                self.state = NodeState.FOLLOWER
            logger.info(f"Received heartbeat, reset state to FOLLOWER for {self.id}")

    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', self.port))
            s.listen()
            s.settimeout(self.server_timeout)
            logger.info(f"Server started on port {self.port}")
            while not self.stop_signal.is_set():
                try:
                    conn, addr = s.accept()
                    thread = threading.Thread(target=self.handle_connection, args=(conn, addr))
                    thread.start()
                    self.threads.append(thread)
                except socket.timeout:
                    continue

    def send_heartbeat(self):
        for peer in self.peers:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(('localhost', peer[1]))
                    s.sendall(heartbeat(self.term))
                    logger.info(f"Node {self.id} sent heartbeat to {peer[0]}")
            except ConnectionRefusedError:
                logger.error(f"Node {self.id} could not connect to {peer[0]}")

    @synchronizer('lock')
    def state_check(self):
        if self.state == NodeState.CANDIDATE:
            self.request_votes()
        elif self.state == NodeState.LEADER:
            self.send_heartbeat()
        else:
            logger.debug(f"Node {self.id} is in state {self.state}")
        return random.randint(1, 5) if self.state == NodeState.CANDIDATE else 5

    def _run(self):
        self.stop_signal.clear()
        thread = threading.Thread(target=self.start_server)
        thread.start()
        self.threads.append(thread)
        while not self.stop_signal.is_set():
            try:
                timeout = self.state_check()
                time.sleep(timeout)
            except Exception as e:
                logger.exception(f"Error in raft daemon for {self.id}:\n{e}")

    def run(self, blocking=False):
        logger.info(f"Node {self.id} starting...")
        thread = threading.Thread(target=self._run, daemon=True)
        self.threads.append(thread)
        thread.start()
        if blocking:
            for t in self.threads:
                t.join()
    
    def stop(self):
        self.stop_signal.set()
        for thread in self.threads:
            thread.join()
        logger.info(f"Node {self.id} stopped")
