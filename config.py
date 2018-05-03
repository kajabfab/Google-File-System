__LOCALHOST__ = 'localhost'
CHANNEL_MSG_SIZE = 1024


class ClientServerConfig:
    HOST = __LOCALHOST__
    PORT = 13131
    ADDR = (HOST, PORT)
    LISTEN = 1


class MasterServerConfig:
    HOST = __LOCALHOST__
    PORT = 14141
    ADDR = (HOST, PORT)

    CHUNK_SIZE = 20
    DELETED_FILE_ROOT = './GFS-File-System/hidden/deleted'
    NUM_CHUNK_SERVERS = 3
    CURRENT_CHUNK_SERVER = 0

    LISTEN = 1 + NUM_CHUNK_SERVERS

    HEARTBEAT = 2


class ChunkServerConfig:
    HOST = __LOCALHOST__
    PORT = 15151
    LISTEN = 2

    LOCAL_FILESYSTEM_ROOT_PARENT = "./GFS-File-System/chunks"

    def __init__(self):
        self.PORT = ChunkServerConfig.PORT
        self.ADDR = (self.HOST, self.PORT)
        ChunkServerConfig.PORT += 10
