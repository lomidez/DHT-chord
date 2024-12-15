"""
Authors: Lisa Lomidze, Kevin Lundeen

This script is designed to query a value from the Chord Distributed Hash Table (DHT).
It takes the port number of an existing Chord node, a player ID, and a year as input,
and retrieves the associated value stored in the DHT.
"""
import sys
from chord_node import Chord, M

def hash_key(player_id, year):
    """
    Generate a SHA-1 hash for the concatenated key (Player ID + Year).
    The hash is reduced to fit within the m-bit Chord ring space.

    :param player_id: The unique player ID.
    :param year: The year associated with the record.
    :return: The hashed value of the key within the m-bit space.
    """
    key = f"{player_id}{year}"
    return Chord.hash(key, M)

if len(sys.argv) != 4:
    print("Usage: python3 get_data.py PORT QB YEAR")
    print("Example:")
    port = 25634  # any running node
    player_id = 'breezyreid/2523928'
    year = 1954
    print(f"python3 get_data.py {port} {player_id} {year}")
    print()
else:
    port = int(sys.argv[1])
    player_id = sys.argv[2]
    year = sys.argv[3]

# Hash the key
hashed_key = hash_key(player_id, year)

# Perform the lookup
node = Chord.lookup_addr(port)
print(f"Looking up key: {hashed_key} ({player_id}, {year}) from node {node}")
result = Chord.get_value(node, hashed_key)

# Print the result
if result:
    print("Result:", result)
else:
    print(f"Key ({player_id}, {year}) not found in the DHT.")


