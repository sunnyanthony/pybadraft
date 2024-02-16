import json
import struct
from enum import Enum, auto
from dataclasses import dataclass

# TODO: gprof or grpc instead

class Request(Enum):
    NONE = 0
    VOTE_REQUEST = 1
    VOTE_GRANTED = 2
    HEARTBEAT = 3


@dataclass
class MetaData:
    type: Request
    term: int
    id: int
    granted: bool

    def pack_data(self):
        return struct.pack('!III', self.type.value, self.term, self.id)
    
    @classmethod
    def unpack_data(cls, packed_data):
        unpacked_data = struct.unpack('!III', packed_data)
        data_obj = cls(Request(unpacked_data[0]), *unpacked_data[1:])
        return data_obj

def load_packet(data) -> MetaData:
    #return json.loads(data.decode())
    return MetaData.unpack_data(data)

def heartbeat(term):
    #return json.dumps({"type": "heartbeat", "term": term}).encode()
    return MetaData(type=Request.HEARTBEAT, term=term).pack_data()

def vote(vote_granted, candidate_id):
    #return json.dumps({"type": "vote_granted", "vote_granted": vote_granted, "candidate_id": candidate_id}).encode()
    return MetaData(type=Request.VOTE_GRANTED, granted=vote_granted, id=candidate_id).pack_data()

def vote_request(term, candidate_id):
    #return json.dumps({"type": "vote_request", "term": term, "candidate_id": candidate_id}).encode()
    return MetaData(type=Request.VOTE_REQUEST, term=term, id=candidate_id).pack_data()