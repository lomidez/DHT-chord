"""
Authors: Lisa Lomidze, Kevin Lundeen

Implementation of the Chord Distributed Hash Table (DHT) protocol.
This module provides functionality for creating, joining, and interacting 
with a distributed network of nodes to efficiently store and retrieve key-value pairs.
"""
import sys
import hashlib
import pickle
import socket
import threading
import time


M = 10  # FIXME 
NODES = 2**M
BUF_SZ = 8192  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n
LOG_HEARTBEAT = 10 #sec, report to log every so often even with no RPC calls
POSSIBLE_HOSTS = ('localhost',)
POSSIBLE_PORTS = (32223, 31267, 25634, 10008, 10058, 
                  10088, 28231, 28234) #FIXME add all needed ports

class ModRange(object):
    """
    Range-like object that wraps around 0 at some divisor using modulo arithmetic.

    >>> mr = ModRange(1, 4, 100)
    >>> mr
    <mrange [1,4)%100>
    >>> 1 in mr and 2 in mr and 4 not in mr
    True
    >>> [i for i in mr]
    [1, 2, 3]
    >>> mr = ModRange(97, 2, 100)
    >>> 0 in mr and 99 in mr and 2 not in mr and 97 in mr
    True
    >>> [i for i in mr]
    [97, 98, 99, 0, 1]
    >>> [i for i in ModRange(0, 0, 5)]
    [0, 1, 2, 3, 4]
    """

    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        # we want to use ranges to make things speedy, but if it wraps around the 0 node, we have to use two
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        elif self.stop == 0:
            self.intervals = (range(self.start, self.divisor),)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.stop, self.divisor)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        for interval in self.intervals:
            if id in interval:
                return True
        return False

    def __len__(self):
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        return ModRangeIter(self, 0, -1)

class ModRangeIter(object):
    """ Iterator class for ModRange """
    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]

class FingerEntry(object):
    """
    Row in a finger table.

    >>> fe = FingerEntry(0, 1)
    >>> fe
    
    >>> fe.node = 1
    >>> fe
    
    >>> 1 in fe, 2 in fe
    (True, False)
    >>> FingerEntry(0, 2, 3), FingerEntry(0, 3, 0)
    (, )
    >>> FingerEntry(3, 1, 0), FingerEntry(3, 2, 0), FingerEntry(3, 3, 0)
    (, , )
    >>> fe = FingerEntry(3, 3, 0)
    >>> 7 in fe and 0 in fe and 2 in fe and 3 not in fe
    True
    """
    def __init__(self, n, k, node=None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2**(k-1)) % NODES
        self.next_start = (n + 2**k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.next_start, self.node)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        return id in self.interval

class ChordNode(object):
    """
    Class representing a node in the Chord Distributed Hash Table (DHT).
    Each node maintains a finger table for efficient routing, a predecessor pointer,
    and a set of keys it is responsible for.
    """
    def __init__(self, port, buddy=None):
        """
        Initializes a Chord node.

        :param port: The port on which this node listens for incoming requests.
        :param buddy: The port of an existing node to join an existing network; None if creating a new network.
        """
        self.node = Chord.lookup_addr(port)
        self.port_before_lookup = port
        self.finger = [None] + [FingerEntry(self.node, k) for k in range(1, M+1)]  # indexing starts at 1
        self.predecessor = None
        self.keys = {}
        self.joined = False
        self.buddy = Chord.lookup_addr(buddy)
        self._stop_print_thread = threading.Event()
        self.start_periodic_print()

    def __repr__(self):
        """
        Compact representation for debugging. 
        >>> node = ChordNode(34023)
        could not use 65528 possible addresses due to hash conflicts (low M?)
        usable addresses: 8
        >>> node.finger[1].node = 3; node.finger[2].node = 3; node.finger[3].node = 6;
        >>> node
        <2: [3, 3, 6]7>
        """
        fingers = ','.join([str(self.finger[i].node) for i in range(1, M+1)])
        return '<{}: [{}]{}>'.format(self.node, fingers, self.predecessor)

    @property
    def successor(self):
        return self.finger[1].node

    @successor.setter
    def successor(self, id):
        self.finger[1].node = id

    def periodic_print(self):
        """
        Periodically prints the node's state each 2 seconds.
        """
        while not self._stop_print_thread.is_set():
            print(self)
            time.sleep(2)

    def start_periodic_print(self):
        """
        Starts a thread that periodically prints the node's state.
        """
        thread = threading.Thread(target=self.periodic_print, daemon=True)
        thread.start()

    def stop_periodic_print(self):
        """
        Stops the thread that prints the node's state.
        """
        self._stop_print_thread.set()

    def serve_forever(self):
        """
        Main loop to handle Remote Procedure Call (RPC) requests.
        """
        listener_thread = threading.Thread(target=self.start_listening)
        listener_thread.start()
        print(f"{self.node}.serve_forever(('localhost', {self.port_before_lookup}))")

    def start_listening(self):
        """
        Listens for incoming TCP connections and handles them.
        """
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('localhost', self.port_before_lookup))
        server.listen(BACKLOG)
        
        while True:
            try:
                client, address = server.accept()
                # Handle each client in a separate thread
                threading.Thread(target=self.handle_rpc, args=(client,)).start()
            except Exception as e:
                print(f"Error in listening loop: {e}")

    def handle_rpc(self, client):
        """
        Processes incoming RPC requests and sends responses.

        :param client: The client socket connection.
        """
        try:
            # Receive data from the client
            data = client.recv(BUF_SZ)
            if not data:
                print("Received empty data. Closing connection.")
                return
            
            # Deserialize the received data (unpack the RPC request)
            rpc_request = pickle.loads(data)
            
            # Extract method name and arguments from the request
            method_name = rpc_request[0]
            args = rpc_request[1:]
            print(f"{self.node}.{method_name}({', '.join(map(str, args))})")
            
            # Default values for arguments if missing
            arg1 = args[0] if len(args) > 0 else None
            arg2 = args[1] if len(args) > 1 else None

            if method_name == 'successor':
                result = self.finger[1].node

            elif method_name == 'predecessor':
                if arg1 is not None:
                    self.predecessor = arg1
                    result = "OK"
                else:
                    result = self.predecessor

            elif method_name == 'update_finger_table':
                self.update_finger_table(arg1, arg2)
                result = "OK"

            elif method_name == 'closest_preceding_finger':
                result = self.closest_preceding_finger(arg1)

            elif hasattr(self, method_name):
                proc_method = getattr(self, method_name)
                if arg1 is not None and arg2 is not None:
                    result = proc_method(arg1, arg2)
                elif arg1 is not None:
                    result = proc_method(arg1)
                else:
                    result = proc_method()

            
            if (method_name == "successor" or method_name == "closest_preceding_finger" or 
                method_name == "find_successor"):
                print(f"\t{self.node}.{method_name}({', '.join(map(str, args))}) --> {result}")

            # Send the result back to the client
            client.sendall(pickle.dumps(result))
        
        except Exception as e:
            print(f"RPC error: {e}")
            error_message = f"Error processing RPC: {e}"
            client.sendall(pickle.dumps({"error": error_message}))
        
        finally:
            client.close()  # Ensure the client connection is closed


    def find_successor(self, id):
        """
        Finds the successor node responsible for a given ID.

        :param id: The ID to find the successor for.
        :return: The node ID of the successor.
        """
        print(f"{self.node}.find_successor({id})")
        np = self.find_predecessor(id)
        successor = self.call_rpc(np, 'successor')
        print(f"\t{self.node}.find_successor({id}) --> {successor}")
        return successor
    

    def find_predecessor(self, id):
        """
        Finds the predecessor node for a given ID.

        :param id: The ID to find the predecessor for.
        :return: The node ID of the predecessor.
        """
        n = self.node  # Start with this node's ID
        while id not in ModRange(n + 1, self.call_rpc(n, 'successor') + 1, NODES):
            n = self.call_rpc(n, 'closest_preceding_finger', id)
        return n
    
    def closest_preceding_finger(self, id):
        """
        Finds the closest preceding finger for a given ID.

        :param id: The ID to find the closest preceding finger for.
        :return: The closest preceding finger's node ID.
        """
        for i in range(M, 0, -1):
            if self.finger[i].node in ModRange(self.node + 1, id, NODES):
                return self.finger[i].node
        return self.node

    def join(self, buddy=None):
        """
        Joins the Chord ring via a buddy node.

        :param buddy: The ID of the buddy node to join through.
        """
        print(f"{self.node}.join({buddy})")
        if buddy:
            self.init_finger_table(self.buddy)
            self.update_others()
            self.joined = True
        else:
            for i, finger in enumerate(self.finger[1:], start=1):
                start, stop = finger.interval.start, finger.interval.stop
                finger.node = self.node
            self.predecessor = self.node
            self.joined = True
            for i, finger in enumerate(self.finger[1:], start=1):
                start, stop = finger.interval.start, finger.interval.stop

    def init_finger_table(self, buddy):
        """
        Initializes the finger table using a buddy node.

        :param buddy: The buddy node ID for initialization.
        """
        print(f"{self.node}.init_finger_table({buddy})")
        self.finger[1].node = self.call_rpc(buddy, 'find_successor', self.finger[1].start)
        self.predecessor = self.call_rpc(self.finger[1].node, 'predecessor')
        self.call_rpc(self.finger[1].node, 'predecessor', self.node)
        
        for i in range(1, M):
            if self.finger[i + 1].start in ModRange(self.node, self.finger[i].node, NODES):
                self.finger[i + 1].node = self.finger[i].node
            else:
                self.finger[i + 1].node = self.call_rpc(buddy, 'find_successor', self.finger[i + 1].start)

    @staticmethod
    def call_rpc(node, method, arg1=None, arg2=None):
        """
        Perform a Remote Procedure Call (RPC) to a specified node.

        :param node: The ID of the target node to send the RPC request to.
        :param method: The method name to be invoked on the target node.
        :param arg1: The first argument for the method (default: None).
        :param arg2: The second argument for the method (default: None).
        :return: The result of the RPC call if successful, otherwise None.
        """
        host, port = Chord.lookup_node(node)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((host, port))
                sock.sendall(pickle.dumps((method, arg1, arg2)))
                response = sock.recv(BUF_SZ)
                return pickle.loads(response)
        except (ConnectionRefusedError, socket.error) as e:
            print(f"RPC failed to {node} ({host}:{port}) - {e}")
            return None

    def update_others(self):
        """ 
        Update all other node that should have this node in their finger tables
        """
        for i in range(1, M+1):  # find last node p whose i-th finger might be this node
            p = self.find_predecessor((1 + self.node - 2**(i-1) + NODES) % NODES) #before3
            self.call_rpc(p, 'update_finger_table', self.node, i)

    def update_finger_table(self, s, i):
        """
        Updates the finger table with a new entry.

        :param s: The node ID to update.
        :param i: The index of the finger table entry to update.
        """
        if (self.finger[i].start != self.finger[i].node
                 and s in ModRange(self.finger[i].start, self.finger[i].node, NODES)):
            print('update_finger_table({},{}): {}[{}] = {} since {} in [{},{})'.format(
                     s, i, self.node, i, s, s, self.finger[i].start, self.finger[i].node))
            self.finger[i].node = s
            print('#', self)
            p = self.predecessor  # get first node preceding myself
            self.call_rpc(p, 'update_finger_table', s, i)
            print(self)
            return str(self)

    def get_value(self, key):
        """
        Retrieves a value for a given key.

        :param key: The key to retrieve the value for.
        :return: The value associated with the key.
        """
        return self.keys.get(key, None)

    def put_value(self, key, value):
        """
        Stores a value for a given key.

        :param key: The key to associate the value with.
        :param value: The value to store.
        """
        self.keys[key] = value

class Chord:
    """
    Class for general utilities in the Chord DHT system.
    """
    node_map = None  # Cache of hashed node addresses to (host, port) tuples

    @staticmethod
    def put_value(node, key, value):
        """
        Store a key-value pair in the DHT by routing to the responsible node.
        """
        # Find the successor node responsible for this key
        successor = Chord.call_rpc(node, 'find_successor', key)
        # Store the key-value pair in the successor node
        Chord.call_rpc(successor, 'put_value', key, value)

    @staticmethod
    def get_value(node, key):
        """
        Retrieve a value from the DHT using a key.
        """
        # Route the request to the node responsible for the key
        successor = Chord.call_rpc(node, 'find_successor', key)
        # Fetch the value from the responsible node
        return Chord.call_rpc(successor, 'get_value', key)

    @staticmethod
    def call_rpc(n, method, arg1=None, arg2=None):
        """
        Perform an RPC call to a specified node.
        """
        host, port = Chord.lookup_node(n)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((host, port))
                sock.sendall(pickle.dumps((method, arg1, arg2)))
                response = sock.recv(BUF_SZ)
                return pickle.loads(response)
        except (ConnectionRefusedError, socket.error) as e:
            print(f"RPC failed to {n} ({host}:{port}) - {e}")
            return None

    @staticmethod
    def lookup_node(node):
        """Retrieve the (host, port) tuple for a given node ID."""
        if Chord.node_map is None:
            Chord.node_map = {}
            for host in POSSIBLE_HOSTS:
                for port in POSSIBLE_PORTS:
                    id = Chord.hash((host, port), M)
                    Chord.node_map[id] = (host, port)
        if node not in Chord.node_map:
            raise KeyError(f"Node {node} is not in Chord's node map.")
        return Chord.node_map[node]

    @staticmethod
    def lookup_addr(port, host='localhost'):
        """
        Compute the hashed node ID for a given address.
        """
        return Chord.hash((host, port), M)

    @staticmethod
    def hash(key, bits=None):
        """
        Hash a key using SHA-1 and reduce it to the required number of bits.
        """
        bits = bits or hashlib.sha1().digest_size * 8
        return int(hashlib.sha1(str(key).encode()).hexdigest(), 16) % (2 ** bits)


if __name__ == '__main__':
    usage = """python3 create_DHT_node.py PORT [BUDDY]
    Idea is to start various processes in separate terminal windows like that:
    $ python3 create_DHT_node.py 34023 &  # initial node (after hashing = 3)
    $ python3 create_DHT_node.py 31488 34023 &  # add node 0 (after hashing) using 3 as a buddy
    $ python3 create_DHT_node.py 7895 31488 &  # add node 1 (after hashing) using 0 as a buddy
    """
    
    if len(sys.argv) < 2:
        print(usage)
        sys.exit(1)
    
    port = int(sys.argv[1])
    buddy = int(sys.argv[2]) if len(sys.argv) > 2 else None  # Buddy can be None for the first node

    print("CHORD: m =,", M)
    print(f"Starting node {port} , in Chord DHT id =  {Chord.lookup_addr(port)}")
    if buddy:
        print(f"Joining via buddy node at port {buddy}")
    else:
        print("Starting a new Chord network (no buddy).")
    
    node = ChordNode(port, buddy)
    node.serve_forever()
    node.join(buddy)
