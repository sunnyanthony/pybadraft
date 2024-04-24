import struct
from enum import Enum
from dataclasses import dataclass

# TODO: gprof or grpc instead

class Request(Enum):
    NONE = 0
    VOTE_REQUEST = 1
    VOTE_GRANTED = 2
    HEARTBEAT = 3
    COMMAND = 4
    USERCOMMAND = 5

@dataclass
class MetaData:
    type: Request = Request.NONE
    term: int = 0
    id: int = 0
    granted: bool = False
    cmd: str = ""

    def pack_data(self):
        return struct.pack('!IIII', self.type.value, self.term, self.id, 1 if self.granted else 0)
    
    @classmethod
    def unpack_data(cls, packed_data):
        unpacked_data = struct.unpack('!IIII', packed_data)
        granted = True if unpacked_data[3] == 1 else False
        data_obj = cls(Request(unpacked_data[0]), unpacked_data[1], unpacked_data[2], granted)
        return data_obj

def load_packet(data) -> MetaData:
    return MetaData.unpack_data(data)

def heartbeat(id, term):
    return MetaData(type=Request.HEARTBEAT, id=id, term=term).pack_data()

def vote(vote_granted, term, id):
    return MetaData(type=Request.VOTE_GRANTED, granted=vote_granted, term=term, id=id).pack_data()

def vote_request(term, candidate_id):
    return MetaData(type=Request.VOTE_REQUEST, term=term, id=candidate_id).pack_data()