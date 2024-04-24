from enum import Enum
from . import packet_pb2

class Request(Enum):
    NONE = packet_pb2.NONE
    VOTE_REQUEST = packet_pb2.VOTE_REQUEST
    VOTE_GRANTED = packet_pb2.VOTE_GRANTED
    HEARTBEAT = packet_pb2.HEARTBEAT
    COMMAND = packet_pb2.COMMAND
    USERCOMMAND = packet_pb2.USERCOMMAND


class MetaData:
    def __init__(self, type=Request.NONE, term=0, id=0, granted=False, cmd=""):
        self.type = type
        self.term = term
        self.id = id
        self.granted = granted
        self.cmd = cmd

    def pack_data(self):
        # Serialize using protobuf
        data = packet_pb2.MetaData()
        data.type = self.type
        data.term = self.term
        data.id = self.id
        data.granted = self.granted
        data.cmd = self.cmd
        return data.SerializeToString()

    @classmethod
    def unpack_data(cls, packed_data):
        # Deserialize using protobuf
        data_obj = packet_pb2.MetaData()
        data_obj.ParseFromString(packed_data)
        instance = cls(
            type=data_obj.type,
            term=data_obj.term,
            id=data_obj.id,
            granted=data_obj.granted,
            cmd=data_obj.cmd
        )
        return instance

def load_packet(data) -> MetaData:
    return MetaData.unpack_data(data)

def heartbeat(id, term):
    return MetaData(type=Request.HEARTBEAT, id=id, term=term).pack_data()

def vote(vote_granted, term, id):
    return MetaData(type=Request.VOTE_GRANTED, granted=vote_granted, term=term, id=id).pack_data()

def vote_request(term, candidate_id):
    return MetaData(type=Request.VOTE_REQUEST, term=term, id=candidate_id).pack_data()
