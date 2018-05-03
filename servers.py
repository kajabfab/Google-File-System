import os
from math import ceil
from functools import reduce
from operator import add, itemgetter
from uuid import uuid1
from time import time
from socket import socket, AF_INET, SOCK_STREAM
from threading import Thread

from config import ClientServerConfig, \
    MasterServerConfig, \
    ChunkServerConfig, \
    CHANNEL_MSG_SIZE


class BaseServer:

    def __init__(self, host, port, listen):
        self.__HOST = host
        self.__PORT = port
        self.__ADDR = (self.__HOST, self.__PORT)
        self.__LISTEN = listen

        self.socket = socket(family=AF_INET, type=SOCK_STREAM)
        self.socket.bind(self.__ADDR)
        self.socket.listen(self.__LISTEN)

        self.log = []

    def get_addr(self):
        return self.__ADDR

    def dump_log(self):
        for entry in self.log:
            print(entry)

    def close(self):
        self.socket.close()

    @staticmethod
    def send_message(message, address):
        sock = socket(family=AF_INET, type=SOCK_STREAM)
        sock.connect(address)
        sock.send(message.encode('ascii'))
        sock.close()

    def recv_message(self):
        sock, addr = self.socket.accept()
        response_str = sock.recv(CHANNEL_MSG_SIZE)
        sock.close()
        self.log.append(response_str)
        return response_str


class ClientServer(BaseServer):

    def __init__(self):
        super().__init__(
            host=ClientServerConfig.HOST,
            port=ClientServerConfig.PORT,
            listen=ClientServerConfig.LISTEN
        )

    def exists(self, filename):

        request = "E#{}".format(str(filename))
        BaseServer.send_message(request, MasterServerConfig.ADDR)

        response_str = self.recv_message()
        response = response_str.decode('ascii')

        if response == 'Y':
            return True
        elif response == 'N':
            return False

    def write(self, filename, data):

        if self.exists(filename):
            self.delete(filename)

        else:

            num_chunks = ClientServer.calc_num_chunks(data)
            request = "W#{}#{}".format(str(filename), str(num_chunks))
            BaseServer.send_message(request, MasterServerConfig.ADDR)

            response_str = self.recv_message()
            response = response_str.decode('ascii').split('#')

            if response[0] == 'OK' and num_chunks == int(response[1]):
                self.write_chunks(response, num_chunks, data)

    def write_chunks(self, response, num_chunks, data):
        chunk_mapping = ClientServer.generate_chunk_mapping(response[2:])

        chunk_size = MasterServerConfig.CHUNK_SIZE
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

        for i in range(num_chunks):
            chunk_uid, chunkserver_addr = chunk_mapping[i]
            request = "W#{}#{}".format(chunk_uid, chunks[i])
            BaseServer.send_message(request, chunkserver_addr)

            response_str = self.recv_message()
            response = response_str.decode('ascii').split()

            if response[0] == 'OK':
                pass

    def read(self, filename):

        if self.exists(filename):
            request = "R#{}".format(str(filename))
            BaseServer.send_message(request, MasterServerConfig.ADDR)

            response_str = self.recv_message()
            response = response_str.decode('ascii').split('#')

            if response[0] == 'OK':
                data = self.read_chunks(response)
                return data
        else:
            raise Exception("READ ERROR: File does not exist: {}".format(filename))

    def read_chunks(self, response):
        data = []

        num_chunks = int(response[1])
        chunk_mapping = ClientServer.generate_chunk_mapping(response[2:])
        for i in range(num_chunks):
            chunk_uid, chunkserver_addr = chunk_mapping[i]
            request = "R#{}".format(chunk_uid)
            BaseServer.send_message(request, chunkserver_addr)

            response_str = self.recv_message()
            response = response_str.decode('ascii').split('#')

            if response[0] == 'OK':
                data.append(response[1])

        return reduce(add, data)

    def append(self, filename, data):

        if self.exists(filename):

            num_chunks = ClientServer.calc_num_chunks(data)
            request = "A#{}#{}".format(str(filename), str(num_chunks))
            BaseServer.send_message(request, MasterServerConfig.ADDR)

            response_str = self.recv_message()
            response = response_str.decode('ascii').split('#')

            if response[0] == 'OK' and num_chunks == int(response[1]):
                self.write_chunks(response, num_chunks, data)
        else:
            raise Exception("APPEND ERROR: File does not exist: {}".format(filename))

    def delete(self, filename):

        if self.exists(filename):
            request = "D#{}".format(str(filename))
            BaseServer.send_message(request, MasterServerConfig.ADDR)

            response_str = self.recv_message()
            response = response_str.decode('ascii').split('#')

            if response[0] == 'OK':
                pass
        else:
            raise Exception("DELETE ERROR: File does not exist: {}".format(filename))

    @staticmethod
    def calc_num_chunks(data):
        size = len(data)
        return int(ceil(size / MasterServerConfig.CHUNK_SIZE))

    @staticmethod
    def generate_chunk_mapping(chunklist):
        chunk_mapping = []
        for x_str in chunklist:
            x = x_str.strip('[]').split(',')
            chunk_uid = x[0]
            y = x[1].strip('()').split('|')
            host = y[0]
            port = int(y[1])
            chunkserver_addr = (host, port)
            chunk_mapping.append((chunk_uid, chunkserver_addr))
        return chunk_mapping


class MasterServer(BaseServer, Thread):

    def __init__(self):
        super().__init__(
            host=MasterServerConfig.HOST,
            port=MasterServerConfig.PORT,
            listen=MasterServerConfig.LISTEN
        )
        Thread.__init__(self)

        self.NUM_CHUNK_SERVERS = MasterServerConfig.NUM_CHUNK_SERVERS
        self.robin = MasterServerConfig.CURRENT_CHUNK_SERVER

        self.filetable = {}
        self.deleted_filetable = {}

        self.chunktable = {}
        self.chunkservers = {}

        self.__chunkservers = []
        self.__init_chunkservers()

        self.num_requests = 0

    def __init_chunkservers(self):
        for i in range(0, self.NUM_CHUNK_SERVERS):
            c = ChunkServer(i)
            self.__chunkservers.append(c)
            self.chunkservers[i] = c.get_addr()
            c.start()

    def run(self):
        while True:
            request_str = self.recv_message()
            request = request_str.decode('ascii').split('#')
            self.num_requests += 1

            response = 'ERR'

            command = request[0]
            if command == 'E':
                filename = request[1]
                response = self.exists(filename)

            elif command == 'W':
                filename = request[1]
                num_chunks = int(request[2])
                self.alloc(filename, num_chunks)

                response = self.read(filename)

            elif command == 'R':
                filename = request[1]
                response = self.read(filename)

            elif command == 'A':
                filename = request[1]
                num_chunks = int(request[2])
                chunk_uids = self.alloc_append_chunks(filename, num_chunks)

                response = self.read(filename, chunk_uids)

            elif command == 'D':
                filename = request[1]
                response = self.delete(filename)

            if command == 'GC':
                chunk_loc = int(request[1])
                num_chunks = int(request[2])
                if num_chunks > 0:
                    chunk_uids = request[3:]
                    response = self.check(chunk_uids)
                    BaseServer.send_message(response, self.chunkservers[chunk_loc])
            else:
                BaseServer.send_message(response, ClientServerConfig.ADDR)

            if self.num_requests % MasterServerConfig.HEARTBEAT == 0:
                pass  # self.full_delete()

    def exists(self, filename):
        if filename in self.filetable.keys():
            response = 'Y'
        else:
            response = 'N'
        return response

    def alloc(self, filename, num_chunks):
        chunk_uids = self.alloc_new_chunks(num_chunks)
        self.filetable[filename] = chunk_uids

    def alloc_new_chunks(self, num_chunks):
        chunk_uids = []
        for i in range(num_chunks):
            chunk_uid = uuid1()
            chunk_loc = self.robin

            self.chunktable[chunk_uid] = chunk_loc

            chunk_uids.append(chunk_uid)
            self.robin = (self.robin + 1) % self.NUM_CHUNK_SERVERS
        return chunk_uids

    def alloc_append_chunks(self, filename, num_chunks):
        chunk_uids = self.alloc_new_chunks(num_chunks)
        self.filetable[filename] += chunk_uids
        return chunk_uids

    def read(self, filename, chunk_uids=None):
        if not chunk_uids:
            chunk_uids = self.filetable[filename]
        num_chunks = len(chunk_uids)

        response = 'OK#{}'.format(str(num_chunks))
        for chunk_uid in chunk_uids:
            chunk_loc = self.chunktable[chunk_uid]
            host, port = self.chunkservers[chunk_loc]
            response += '#[{},({}|{})]'.format(chunk_uid, host, str(port))
        return response

    def delete(self, filename):
        chunk_uids = self.filetable[filename]
        del self.filetable[filename]

        timestamp = repr(time())
        deleted_filename = "{}/{}/{}".format(MasterServerConfig.DELETED_FILE_ROOT, timestamp, filename)
        self.filetable[deleted_filename] = chunk_uids
        self.deleted_filetable[deleted_filename] = chunk_uids

        response = 'OK#{}#{}'.format(filename, deleted_filename)
        return response

    def full_delete(self):
        for deleted_filename in self.deleted_filetable:
            for chunk_uid in self.deleted_filetable[deleted_filename]:
                del self.chunktable[chunk_uid]
            del self.filetable[deleted_filename]

    def check(self, chunk_uids):
        old_chunk_uids = []
        for chunk_uid in chunk_uids:
            if chunk_uid not in list(map(str, list(self.chunktable.keys()))):
                old_chunk_uids.append(chunk_uid)

        response = 'GC#{}'.format(str(len(old_chunk_uids)))
        for chunk_uid in old_chunk_uids:
            response += '#{}'.format(chunk_uid)
        return response

    def dump_metadata(self):
        print()
        print("---- FileTable ----")
        for filename, chunk_uids in self.filetable.items():
            print("::::")
            print("Filename: {}".format(filename))
            print("Chunks: {}".format(chunk_uids))
            print("::::")
        print("-------------------")
        print()
        print("---- ChunkTable ----")
        print("Chunk_loc | Chunk_uid")
        for chunk_uid, chunk_loc in sorted(self.chunktable.items(), key=itemgetter(1)):
            print(chunk_uid, str(chunk_loc))
        print("--------------------")
        print()

    def close(self):
        super().close()
        for c in self.__chunkservers:
            c.close()


class ChunkServer(BaseServer, Thread):

    def __init__(self, location):
        config = ChunkServerConfig()
        super().__init__(
            host=config.HOST,
            port=config.PORT,
            listen=config.LISTEN
        )
        Thread.__init__(self)

        self.location = location
        self.chunktable = {}
        self.local_filesystem_root = '{}/{}'.format(config.LOCAL_FILESYSTEM_ROOT_PARENT, str(location))
        if not os.access(self.local_filesystem_root, os.W_OK):
            os.makedirs(self.local_filesystem_root)

    def run(self):
        while True:
            request_str = self.recv_message()
            request = request_str.decode('ascii').split('#')

            response = 'ERR'

            command = request[0]
            if command == 'W':
                chunk_uid = request[1]
                chunk = request[2]
                response = self.write(chunk_uid, chunk)

            elif command == 'R':
                chunk_uid = request[1]
                chunk = self.read(chunk_uid)
                response = 'OK#{}'.format(chunk)

            if command == 'GC':
                num_chunks = int(request[1])
                if num_chunks > 0:
                    chunk_uids = request[2:]
                    self.delete(chunk_uids)
            else:
                BaseServer.send_message(response, ClientServerConfig.ADDR)
                self.check()

    def write(self, chunk_uid, chunk):
        local_filename = self.chunk_filename(chunk_uid)
        with open(local_filename, 'w') as f:
            f.write(chunk)
        self.chunktable[chunk_uid] = local_filename
        return 'OK'

    def read(self, chunk_uid):
        local_filename = self.chunk_filename(chunk_uid)
        with open(local_filename, 'r') as f:
            chunk = f.read()
        return chunk

    def check(self):
        chunk_uids = self.chunktable.keys()
        request = "GC#{}#{}".format(str(self.location), str(len(chunk_uids)))
        for chunk_uid in chunk_uids:
            request += '#{}'.format(chunk_uid)

        BaseServer.send_message(request, MasterServerConfig.ADDR)

    def delete(self, chunk_uids):
        for chunk_uid in chunk_uids:
            local_filename = self.chunktable[chunk_uid]
            del self.chunktable[chunk_uid]
            os.remove(local_filename)

    def chunk_filename(self, chunk_uid):
        local_filename = "{}/{}.part".format(self.local_filesystem_root, str(chunk_uid))
        return local_filename
