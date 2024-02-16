import json
from enum import Enum, auto
from dataclasses import dataclass

# TODO: gprof or grpc instead

class Request(Enum):
    VOTE_REQUEST = auto()
    VOTE_GRANTED = auto()
    HEARTBEAT = auto()


@dataclass
class data:
    type: Request
    term: int
    id: int

def load_packet(data):
    return json.loads(data.decode())

def heartbeat(term):
    return json.dumps({"type": "heartbeat", "term": term}).encode()

def vote(vote_granted, candidate_id):
    return json.dumps({"type": "vote_granted", "vote_granted": vote_granted, "candidate_id": candidate_id}).encode()

def vote_request(term, candidate_id):
    return json.dumps({"type": "vote_request", "term": term, "candidate_id": candidate_id}).encode()