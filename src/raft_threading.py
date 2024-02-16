import socket
import threading
import time
import json
from src.roles import NodeState

class RaftNode:
    def __init__(self, id, port, peers):
        self.id = id
        self.port = port
        self.peers = peers
        self.state = NodeState.FOLLOWER
        self.term = 0
        self.voted_for = None
        self.votes_received = 0
        self.lock = threading.Lock()

    def become_leader(self):
        self.state = NodeState.LEADER
        print(f"{self.id} is now the leader for term {self.term}.")

    def request_votes(self):
        with self.lock:
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
                s.sendall(json.dumps({"type": "vote_request", "term": self.term, "candidate_id": self.id}).encode())
                response = s.recv(1024).decode()
                response_data = json.loads(response)
                if response_data.get("vote_granted"):
                    with self.lock:
                        self.votes_received += 1
                        if self.votes_received > len(self.peers) / 2:
                            self.become_leader()
            except ConnectionRefusedError:
                print(f"{self.id} could not connect to {peer[0]}")

    def handle_vote_request(self, data, conn):
        with self.lock:
            term = data['term']
            candidate_id = data['candidate_id']
            vote_granted = False
            if (self.term < term) and (self.voted_for is None or self.voted_for == candidate_id):
                self.term = term
                self.voted_for = candidate_id
                self.state = NodeState.FOLLOWER
                vote_granted = True
            conn.sendall(json.dumps({"vote_granted": vote_granted}).encode())

    def handle_connection(self, client_socket, addr):
        try:
            client_socket.settimeout(5)
            data = client_socket.recv(1024)
            if not data:
                return
            message = json.loads(data.decode())
            if message["type"] == "vote_request":
                self.handle_vote_request(message, client_socket)
        except socket.timeout:
            print("Connection timeout")
        finally:
            with self.lock:
                self.state = NodeState.CANDIDATE
            client_socket.close()

    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', self.port))
            s.listen()
            while True:
                conn, addr = s.accept()
                thread = threading.Thread(target=self.handle_connection, args=(conn, addr))
                thread.start()

    def send_heartbeat(self):
        with self.lock:
            for peer in self.peers:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect(('localhost', peer[1]))
                        s.sendall(str(self.term).encode())
                        print(f"Node {self.id} sent heartbeat to {peer[0]}")
                except ConnectionRefusedError:
                    print(f"Node {self.id} could not connect to {peer[0]}")


    def run(self):
        threading.Thread(target=self.start_server).start()
        while True:
            with self.lock:
                if self.state == NodeState.CANDIDATE:
                    self.request_votes()
                elif self.state == NodeState.LEADER:
                    self.send_heartbeat()
            time.sleep(5)