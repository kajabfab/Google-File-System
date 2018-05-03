CLIENT-SERVER MSG REQUESTS

Read() -> R [Filename] 

Write() -> W [Filename] [Num_chunks]

Exist() -> E [Filename]

Append() -> A [Filename] [Num_chunks]

Delete() -> D [Filename]

SERVER-CLIENT MSG RESPONSES

Write() -> OK [Num_chunks] (,) (,) ... \
           ERR

Append() -> OK [Num_chunks] (,) (,) ... \
            ERR

Delete() -> [OK/ERR]

Exist() -> [Y/N]

Read() -> OK [Num_chunks] (,) (,) ... \
          ERR

CLIENT-CHKSVR MSG REQUESTS

Read() -> R [Chunk_id]

Write() -> W [Chunk_id] [Chunk]

CHKSVR-CLIENT MSG RESPONSES

Read() -> OK [Chunk] \
          ERR

Write() -> [OK/ERR]

CHKSVR-SERVER MSG REQUESTS

Garbage_collection() ->  GC [Location] [Num_chunks] [Chunk_ids]

SERVER-CHKSVR MSG RESPONSES

Garbage_collection() ->  GC [Num_chunks] [Chunk_ids]
