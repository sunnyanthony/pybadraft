

from pathlib import Path
import pickle
import struct


class Log(list):
    def __init__(self, *args, **kwargs):
        """
        Initializes the Log class, which extends Python's built-in
        list to include file storage features.
        """
        self.storage = kwargs.pop('storage', None)
        super().__init__(*args, **kwargs)
        self.seek = 0
        if self.storage:
            self.initialize_file()
            self.reload()
    
    def initialize_file(self):
        """ Initializes a new file for storing objects if one does not
        already exist. It writes a zero count to the file. """
        if Path(self.storage).exists():
            return
        with open(self.storage, 'wb') as file:
            file.write(struct.pack('i', 0))  # Write initial counter value
            self.seek = file.tell()  # Update self.seek to file end position

    def reload(self):
        """ Reloads objects from storage, clearing the current list and
        replacing its contents with stored objects. """
        self.clear()  # Clear the current list
        if Path(self.storage).exists():
            with open(self.storage, 'rb') as file:
                count = struct.unpack('i', file.read(4))[0]
                for _ in range(count):
                    self.append(pickle.load(file))
                    self.seek = file.tell()  # Update self.seek to current read position

    def append(self, obj):
        """ Overrides the append method to add an object to the list
        and to the file storage. """
        super().append(obj)
        self.append_object(obj)

    def append_object(self, obj):
        """ Appends an object to the file and updates the object
        count in the file header. """
        with open(self.storage, 'rb+') as file:
            count = struct.unpack('i', file.read(4))[0]
            file.seek(self.seek)  # Position to the end of last write
            pickle.dump(obj, file)
            self.seek = file.tell()  # Update self.seek to the end of new write
            file.seek(0)
            file.write(struct.pack('i', count + 1))  # Update the object count in the file