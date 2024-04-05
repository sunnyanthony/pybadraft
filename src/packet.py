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
    term: int = 0
    id: int = 0
    granted: bool = False

    def pack_data(self):
        return struct.pack('!III', self.type.value, self.term, self.id)
    
    @classmethod
    def unpack_data(cls, packed_data):
        unpacked_data = struct.unpack('!III', packed_data)
        data_obj = cls(Request(unpacked_data[0]), *unpacked_data[1:])
        return data_obj

def load_packet(data) -> MetaData:
    return MetaData.unpack_data(data)

def heartbeat(id):
    return MetaData(type=Request.HEARTBEAT, id=id).pack_data()

def vote(vote_granted, term):
    return MetaData(type=Request.VOTE_GRANTED, granted=vote_granted, term=term).pack_data()

def vote_request(term, candidate_id):
    return MetaData(type=Request.VOTE_REQUEST, term=term, id=candidate_id).pack_data()