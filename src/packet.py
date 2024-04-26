from enum import Enum
from . import packet_pb2
from .packet_pb2 import Request


class MetaData:
    def __init__(self, type=Request.NONE, term=0, id=0, granted=False, cmd=None, ip=None, port=None, service=None):
        self.type = type
        self.term = term
        self.id = id
        self.granted = granted
        self.cmd = cmd
        self.service = service
        #self.ip = ip
        #self.port = port

    def pack_data(self):
        # Serialize using protobuf
        data = packet_pb2.MetaData()
        data.type = self.type.value
        data.term = self.term
        data.id = self.id
        data.granted = self.granted
        if self.cmd:
            data.cmd = self.cmd
        elif self.service:  # If ip and port are provided, use them in the endpoint
            data.endpoint.service = self.service
            #data.endpoint.port = self.port
        return data.SerializeToString()

    @classmethod
    def unpack_data(cls, packed_data):
        # Deserialize using protobuf
        data_obj = packet_pb2.MetaData()
        data_obj.ParseFromString(packed_data)
        # Check which oneof field is set
        service = None
        if data_obj.HasField('cmd'):
            cmd = data_obj.cmd
            #ip = None
            #port = None
        elif data_obj.HasField('endpoint'):
            service = data_obj.endpoint.service
            #port = data_obj.endpoint.port
            cmd = None
        else:
            cmd = None
            #ip = None
            #port = None

        return cls(
            type=Request(data_obj.type),
            term=data_obj.term,
            id=data_obj.id,
            granted=data_obj.granted,
            cmd=cmd,
            service=service
            #ip=ip,
            #port=port
        )

def load_packet(data) -> MetaData:
    return MetaData.unpack_data(data)

def heartbeat(id, term, service = "localhost:1234"):
    return MetaData(type=Request.HEARTBEAT, id=id, term=term, service=service).pack_data()

def vote(vote_granted, term, id):
    return MetaData(type=Request.VOTE_GRANTED, granted=vote_granted, term=term, id=id).pack_data()

def vote_request(term, candidate_id):
    return MetaData(type=Request.VOTE_REQUEST, term=term, id=candidate_id).pack_data()
