"""
Authors: Lisa Lomidze, Kevin Lundeen

This script populates a running Chord Distributed Hash Table (DHT) with data from a specified CSV file.
Each row in the file is hashed into a unique key, and the corresponding row data is stored as a value in the DHT.
"""
import sys
import os
import csv
from chord_node import Chord, M

def populate_from_qd_file(port, filename, max_rows=None):
    """
    Populates the DHT with data from a CSV file.

    :param port: The port of a running Chord node to connect to.
    :param filename: Path to the CSV file containing data to populate.
    :param max_rows: Maximum number of rows to process from the file. If None, all rows are processed.
    """
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        headers = next(reader) 
        for i, row in enumerate(reader):
            if max_rows and i >= max_rows:
                break
            key = f"{row[0]}{row[3]}"
            hashed_key = Chord.hash(key, M)
            value = {header: value for header, value in zip(headers, row)}
            Chord.put_value(Chord.lookup_addr(port), hashed_key, value)


if __name__ == '__main__':
    if len(sys.argv) not in (3, 4):
        print("Usage: python3 chord_populate.py PORT FILENAME [MAX_ROWS]")
        print("Example")
        port = 25634 #any running node
        filename = "test.csv" #FIXME: add needed csv file
        rows = 10
        print("python3 add_data.py {} {} {}".format(port, filename, rows))
    else:
        port = int(sys.argv[1])
        filename = os.path.expanduser(sys.argv[2])
        rows = None if len(sys.argv) < 4 else int(sys.argv[3])
    
    populate_from_qd_file(port, filename, rows)