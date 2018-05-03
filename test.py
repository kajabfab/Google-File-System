from servers import *

master = MasterServer()
master.start()

client = ClientServer()

client.write('lorem', "Lorem Ipsum is simply dummy text of the printing and typesetting industry. ")
print(client.read('lorem'))

master.dump_metadata()

client.append('lorem', "Lorem Ipsum has been the industry's standard dummy text.")
print(client.read('lorem'))

master.dump_metadata()

client.delete('lorem')

master.dump_metadata()

# noinspection PyBroadException
try:
    print(client.read('lorem'))
except:
    print("Read Failed")

print()
print("--- Master Log ---")
master.dump_log()
print("------------------")
print()

print("--- Client Log ---")
client.dump_log()
print()

master.join()
master.close()
client.close()
