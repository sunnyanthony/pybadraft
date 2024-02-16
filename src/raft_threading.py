import socket
import threading
import time
import random
from src.decorator import synchronizer
from src.roles import NodeState
from src.packet import vote_request, vote, load_packet, heartbeat

class RaftNode:
    def __init__(self, id, port, peers):
        self.id = id
        self.port = port
        self.peers = peers
        self.state = NodeState.FOLLOWER
        self.term = 0
        self.voted_for = None
        self.votes_received = 0
        self.lock = threading.RLock() # avoid deadlock

    @synchronizer('lock')
    def become_leader(self):
        if self.state == NodeState.CANDIDATE and self.votes_received > len(self.peers) / 2:
            self.state = NodeState.LEADER
            print(f"{self.id} is now the leader for term {self.term}.")

    @synchronizer('lock')
    def request_votes(self):
        self.state = NodeState.CANDIDATE
        self.term += 1
        self.voted_for = self.id
        self.votes_received = 1
        print(f"{self.id} becomes a candidate and starts election for term {self.term}.")

        for peer in self.peers:
            thread = threading.Thread(target=self.send_vote_request, args=(peer,))
            thread.start()

    def send_vote_request(self, peer):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect(('localhost', peer[1]))
                s.sendall(vote_request(self.term, self.id))
                data = s.recv(1024)
                response_data = load_packet(data)
                if response_data.get("vote_granted"):
                    self.increment_votes()
            except ConnectionRefusedError:
                print(f"{self.id} could not connect to {peer[0]}")

    @synchronizer('lock')
    def increment_votes(self):
        self.votes_received += 1
        self.become_leader()

    @synchronizer('lock')
    def handle_vote_request(self, data, conn):
        term = data['term']
        candidate_id = data['candidate_id']
        vote_granted = False
        if (self.term < term) and (self.voted_for is None or self.voted_for == candidate_id):
            self.term = term
            self.voted_for = candidate_id
            self.state = NodeState.FOLLOWER
            vote_granted = True
        conn.sendall(vote(vote_granted, self.term))

    def handle_connection(self, client_socket, addr):
        try:
            while True:
                timeout = random.randint(6, 10)
                client_socket.settimeout(timeout)
                data = client_socket.recv(1024)
                if not data:
                    break
                message = load_packet(data)
                if message["type"] == "vote_request":
                    self.handle_vote_request(message, client_socket)
                elif message["type"] == "heartbeat":
                    self.reset_state_on_heartbeat(message)
        except socket.timeout:
            print("Connection timeout")
        finally:
            client_socket.close()

    @synchronizer('lock')
    def reset_state_on_heartbeat(self, message):
        if message["term"] >= self.term:
            self.term = message["term"]
            if self.state == NodeState.CANDIDATE:
                self.state = NodeState.FOLLOWER

    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', self.port))
            s.listen()
            while True:
                conn, addr = s.accept()
                thread = threading.Thread(target=self.handle_connection, args=(conn, addr))
                thread.start()

    def send_heartbeat(self):
        for peer in self.peers:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(('localhost', peer[1]))
                    s.sendall(heartbeat(self.term))
                    print(f"Node {self.id} sent heartbeat to {peer[0]}")
            except ConnectionRefusedError:
                print(f"Node {self.id} could not connect to {peer[0]}")

    @synchronizer('lock')
    def state_check(self):
        if self.state == NodeState.CANDIDATE:
            self.request_votes()
        elif self.state == NodeState.LEADER:
            self.send_heartbeat()
        return random.randint(1, 5) if self.state == NodeState.CANDIDATE else 5

    def run(self):
        threading.Thread(target=self.start_server).start()
        while True:
            try:
                timeout = self.state_check()
                time.sleep(timeout)
            except Exception as e:
                print(f"Error in main thread:\n{e}")