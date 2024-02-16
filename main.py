from src.raft import RaftNodeBase
import argparse
import uuid

def generate_id():
    """Generate a random ID within the 32-bit integer range."""
    return uuid.uuid4().int & ((1<<32) - 1)

parser = argparse.ArgumentParser(description="Initialize a Raft node")
parser.add_argument("-i", "--id", type=int, default=-1,
                    metavar="[0-2147483646]",
                    help="ID for this server")
parser.add_argument("-p", "--port", type=int, default=8778,
                    help="TCP port number")
parser.add_argument("-t", "--type", type=str,
                    help="Type of the Raft node")
parser.add_argument('-s', '--servers', nargs='*',
                    type=str, default=[],
                    help="List of server addresses")

if __name__ == "__main__":
    args = parser.parse_args()

    if args.id < 0:
        args.id = generate_id()

    RaftClass = RaftNodeBase.create_raft(args.type)
    RaftNode = RaftClass(args.id, args.port, args.servers)
    # Initialize or start the Raft node as needed
