import hashlib
import pymmh3
from pickle_hash import hash_code_hex
import pickle
from server_config import NODES

class HRWNodeRing:

    def __init__(self, servers, size):
        self.servers = servers
        self.size = size
        self.data = self.initialize_data()
        self.data_count = 0

    def initialize_data(self):
        temp = {}
        for server in self.servers:
            temp[server['port']] = []
        return temp

    def get_node(self, key):
        object_bytes = pickle.dumps(self.servers[0])
        hash_code = hash_code_hex(object_bytes)
        highestWeight = self.hash_func(hash_code, self.servers[0]['port'])
        highestNode = self.servers[0]
        for server in self.servers:
            obj_byte = pickle.dumps(server)
            hc = hash_code_hex(obj_byte)
            weight = self.hash_func(key, hc)
            if weight > highestWeight:
                highestWeight = weight
                highestNode = server
        #print("Candidate highest weight/node: " + str(highestWeight) + " , " + str(highestNode))
        self.data[highestNode['port']].append("data")
        self.data_count += 1

    def hash_func(self, key, node):
        return pymmh3.hash(key + str(node))

    def print_distribution(self):
        for key in self.data.keys():
            print("Server: " + str(key) + ", " + "# of data: " + str(len(self.data[key])) + ", " + "Distribution: " + str(float(len(self.data[key])/self.data_count) * 100) + "%\n")


def testHRW():
    print("---------------------------------------HRW Hashing---------------------------------------")
    n = HRWNodeRing(servers=NODES, size=8)
    n.get_node("testingtesting123")
    n.get_node("sdkflkfjlsdkjf")
    n.get_node("w3khjdkfjhsdkfjhdskf")
    n.get_node("sdflkjwelkfjdslfdsf")
    n.get_node("what the hell coronavirus crap is going on")
    n.get_node("omaewamoushindeiru")
    n.get_node("jet fuel cant melt steel beams")
    n.get_node("testing123")
    n.get_node("sdkfsf3vvvvjf")
    n.get_node("poijipjknljpojsdkfjhdskf")
    n.get_node("sdflksdfseffdsf")
    n.get_node("cao ni ma DE BI WO CAO")
    n.get_node("NANITHEOMAEWFJSDHFKEJW")
    n.get_node("7/11 was a slurpy job")
    n.get_node("michael jorda")
    n.get_node("the last dance")
    n.get_node("steph curry > lebron")
    n.get_node("klay thompson is my favorite player")
    n.get_node("ssdfsfvjf")
    n.get_node("poijip0t0490984023984029urioeojsdkfjhdskf")
    n.get_node("sdflk43534fdsf")
    n.get_node("caocaomeimeisma motwerrtrgrgd going on")
    n.get_node("MORIARITY")
    n.get_node("WAKE UP AND SEMELFKSDb")
    n.get_node("the first dance")
    n.get_node("stephen currysan > lebron")
    n.get_node("is my favorite player")
    n.get_node("2039482309875$(*%&(*#@-29urioeojsdkfjhdskf")
    n.get_node("sdfsdlfk4$#$#")
    n.get_node("youve activated my trap card")
    n.get_node("MAYPOSUTOREEEEEE")
    n.get_node("united center chicago bulls")
    n.print_distribution()



class ConsistentNodeRing():

    def __init__(self, servers, num_nodes, vnodes):
        self.servers = servers #servers themselves
        self.server_keys = self.create_server_keys()
        self.server_node_loc = [] #sorted list of server node indexes
        self.size = num_nodes #n, size of space
        self.virtual_nodes = vnodes #num virtual nodes per server
        self.nodes = {} #key is node index, value is hased server
        self.server_keys_dict = {} #key is hashed server, val is node index
        self.data = {}
        self.set_server_nodes()
        print("--------------------------------------SERVER NODES--------------------------------------")
        print(str(self.server_node_loc))
        print("----------------------------------------------------------------------------------------")

    def create_server_keys(self):
        temp = []
        for server in self.servers:
            object_bytes = pickle.dumps(server)
            hash_code = hash_code_hex(object_bytes)
            temp.append(hash_code)
        return temp

    def print_server_keys(self): #print hashed server keys
        print(self.server_keys)

    def print_server_nodes(self):
        print("Index -> Hash")
        print(self.nodes)
        print("Hash -> Index")
        print(self.server_keys_dict)


    def set_server_nodes(self):
        for key in self.server_keys:
            for i in range(self.virtual_nodes):
                count = 0
                while True:
                    node_index = pymmh3.hash(key, i + count) % self.size
                    if node_index in self.nodes:
                        count += 1
                    else:
                        self.nodes[node_index] = key
                        self.server_node_loc.append(node_index)
                        self.server_keys_dict[key] = node_index
                        break
        self.server_node_loc.sort()

    def get_node(self, key):
        node_index = pymmh3.hash(key) % self.size
        # do server_node walk
        for idx, node in enumerate(self.server_node_loc):
            if node_index <= node: #[5, 14, 15, 23, 26, 38, 51, 53, 66, 81, 86, 90, 95, 98]
                print("1 Data sharded at node_index " + str(node_index) + " serviced by server at node_index: " + str(node) + "\n")
                if idx == len(self.server_node_loc) - 1:
                    print("1.1 Data replicated at node_index " + str(self.server_node_loc[0]) + "\n")
                else:
                    print("1.2 Data replicated at node_index " + str(self.server_node_loc[idx + 1]) + "\n")
                return
        # not in between, so must be on the first
        print("2 Data sharded at node_index " + str(node_index) + " serviced by server at node_index " + str(self.server_node_loc[0]) + "\n")
        print("2.1 Data replicated at server at node_index " + str(self.server_node_loc[1]) + "\n")

    def remove_server(self, key):
        if key in self.server_keys:
            server_node = self.server_keys_dict[key]
            idx = self.server_node_loc.index(server_node)
            if idx == len(self.server_node_loc) - 1:
                rehash = 0
            else:
                rehash = idx + 1
            print("Server " + key + " at node_index " + str(server_node) + " removed from ring.\n")
            print("All values from server " + key + " rehashed to server " + str(self.nodes[self.server_node_loc[rehash]]) + " at node_idx " + str(self.server_node_loc[rehash]) + ".\n")

            self.server_keys.remove(key)
            temp = self.server_keys_dict.pop(key, None)
            self.nodes.pop(temp, None)
            self.server_node_loc.remove(temp)
        else:
            print("Server does not exist.")

    def add_server(self, key):
        if key not in self.server_keys:
            server_node_index = pymmh3.hash(key) % self.size
            self.server_keys_dict[key] = server_node_index
            self.server_keys.append(key)
            self.nodes[server_node_index] = key
            self.server_node_loc.append(server_node_index)
            self.server_node_loc.sort()

    def print_keys_and_nodes(self):
        print("List of server keys: ")
        print(self.print_server_keys)
        print("Server key dict mapped to node_index: ")
        print(self.server_keys_dict)
        print("Node indexes of servers: ")
        print(self.nodes)
        print("Server nodes indexes in sorted ascending order: ")
        print(self.server_node_loc)


def testConsistentHashing():
    print("---------------------------------------Consistent Hashing---------------------------------------\n")
    ring = ConsistentNodeRing(servers=NODES, num_nodes=500, vnodes=2)
    ring.get_node("what the hell coronavirus crap is going on")
    ring.get_node("omaewamoushindeiru")
    ring.get_node("jet fuel cant melt steel beams")
    ring.remove_server('2f5b46fc21ebbfe4d4f90345bd880564')
    ring.add_server('2f5b46fc21ebbfe4d4f90345bd880564')


#ring of possible node values (0 - M-1)
#you hash requests to the ring
# you hash servers to the same ring
# so any request just goes clockwise to find the nearest server to service it
# but this can lead to skewed server load
# a better approach is to hash the servers multiple times to get more than one virtual node


def main():
    testHRW()
    testConsistentHashing()

main()
