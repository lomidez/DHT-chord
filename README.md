# Distributed Hash Table - Chord Protocol Implementation

This project is a scalable implementation of the Chord Distributed Hash Table (DHT) system in Python, designed for efficient distributed data storage and retrieval in a peer-to-peer network.

## Features

### Distributed Architecture
- Implements the Chord protocol based on the original 2001 Stoica et al. paper.
- Nodes dynamically join the network, updating finger tables to maintain consistency.
- Fault-tolerant and scalable design using consistent hashing.

### Advanced Communication
- Utilizes **socket programming** with TCP for inter-node communication.
- Supports multithreaded Remote Procedure Calls (RPC) for concurrent requests.

### Data Management
- Stores and retrieves key-value pairs using composite keys with SHA-1 hashing.
- Modular arithmetic ensures efficient data partitioning across the distributed system.

### Example Data
- Populates the DHT with player statistics, using player ID and year as composite keys.

## Technologies Used
- **Languages/Frameworks**: Python, Socket Programming, Multithreading
- **Development Practices**: Distributed Systems, Peer-to-Peer Networking
- **Other Tools**: Modular Arithmetic

## Usage

### 1. Start a Chord Node
- **Create a New Network**:
  ```bash
  python3 create_DHT_node.py <PORT>
  ```

- **Join an Existing Network**:
  ```bash
  python3 create_DHT_node.py <PORT> <BUDDY_PORT>
  ```

### 2. Populate the DHT with data
- Populate with data:
  ```bash
  python3 add_data.py <NODE_PORT> <CSV_FILE>
  ```

### 3. Query the DHT
- Retrieve a record:
  ```bash
  python3 get_data.py <NODE_PORT> <PLAYER_ID> <YEAR>
  ```

## Contributors

- **Kevin Lundeen**
- **Lisa Lomidze**


## Acknowledgments
- Based on ["Chord: A Scalable Peer-to-Peer Lookup Service for Internet Applications"](https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf) by Stoica et al. (2001).




