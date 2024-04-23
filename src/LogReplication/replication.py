
from dataclasses import dataclass

@dataclass
class CMD:
    length: int
    data: bytearray

@dataclass
class LogEntry:
    term: int
    command: CMD
    def __init__(self, term: int, command: CMD):
        self.term = term
        self.command = command

def is_lock_cmd(command: CMD):
    return True

def get_key(command: CMD):
    return ""


class LogManager:
    def __init__(self, log_callback, command_callback, commit_callback, persistent_storage):
        self.log = Log(storage = persistent_storage)
        self.set = set()
        self.log_cb = log_callback
        self.cmd_cb = command_callback
        self.commit_cb = commit_callback

    def append_entry(self, term, command):
        if is_lock_cmd(command):
            key = get_key(command)
            if key in self.set:
                return False
            self.set.add(key)
            return True
        entry = LogEntry(term, command)
        self.log.append(entry)
        self.persist_log()

    def get_entry(self, index=-1):
        if index < len(self.log):
            return self.log[index]
        return None

    def persist_log(self):
        """ Store the log into persistent storage"""
        pass

    def replicate_log(self, follower_peers):
        """ send log to each peer """
        for peer in follower_peers:
            self.send_log_entries(peer)

    def send_log_entries(self, peer):
        """..."""
        ...
        