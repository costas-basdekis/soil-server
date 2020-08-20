#!/usr/bin/env python3
import datetime
import itertools
import time

import bluetooth as bt


BLUETOOTH_NAME = 'HC-05'
SLEEP_TIME = datetime.timedelta(seconds=1)
SEARCH_INTERVAL = datetime.timedelta(minutes=1)


def main():
    socket_by_address = {}
    buffer_by_socket = {}
    try:
        while True:
            find_and_connect(socket_by_address, buffer_by_socket)
            time_since_last_search = datetime.timedelta(seconds=0)
            while time_since_last_search < SEARCH_INTERVAL:
                receive_and_display_data(socket_by_address, buffer_by_socket)
                time.sleep(SLEEP_TIME.total_seconds())
                time_since_last_search += SLEEP_TIME
    finally:
        close_all_connections(socket_by_address, buffer_by_socket)


def receive_and_display_data(socket_by_address, buffer_by_socket):
    for address, socket in list(socket_by_address.items()):
        new_data = get_new_data(socket)
        if not new_data:
            remove_connection_if_closed(address, socket, socket_by_address, buffer_by_socket)
            continue
        new_lines = append_socket_data(socket, new_data, buffer_by_socket)
        for line in new_lines:
            print('>{}: {}'.format(address[-2:], line.decode()))


def append_socket_data(socket, new_data, buffer_by_socket):
    buffer_by_socket[socket] += new_data
    *new_lines, buffer_by_socket[socket] = buffer_by_socket[socket].split(b'\r\n')
    return new_lines


def remove_connection_if_closed(address, socket, socket_by_address, buffer_by_socket):
    if not is_socket_still_connected(socket):
        print("{} disconnected".format(address))
        del socket_by_address[address]
        del buffer_by_socket[socket]


def close_all_connections(socket_by_address, buffer_by_socket):
    for address, socket in list(socket_by_address.items()):
        if socket:
            socket.close()
        del socket_by_address[address]
        del buffer_by_socket[socket]


def find_and_connect(socket_by_address, buffer_by_socket):
    print("Finding devices")
    mac_addresses_by_name = get_mac_addresses_by_name()
    soil_addresses = mac_addresses_by_name.get(BLUETOOTH_NAME, [])
    print("Got {} names, {} for {}".format(len(mac_addresses_by_name), len(soil_addresses), BLUETOOTH_NAME))
    for address in soil_addresses:
        socket_by_address.setdefault(address, None)
    create_connections(socket_by_address)
    print("Connected to {} devices".format(len(socket_by_address.values())))
    for socket in socket_by_address.values():
        buffer_by_socket.setdefault(socket, b"")


def is_socket_still_connected(socket):
    try:
        socket.getpeername()
        return True
    except bt.BluetoothError as e:
        if e.errno == 107:
            return False
        raise


def get_new_data(socket):
    new_data = b""
    while True:
        try:
            new_data += socket.recv(1024)
        except bt.BluetoothError:
            break
    return new_data


def create_connections(socket_by_address):
    for address, socket in socket_by_address.items():
        if not socket:
            socket = socket_by_address[address] = bt.BluetoothSocket(bt.RFCOMM)
            socket.connect((address, 1))
            socket.setblocking(False)
            print("Connected to {}".format(address))


def get_mac_addresses_by_name():
    mac_addresses_by_name = {
        name: [address for _, address in names_and_addresses]
        for name, names_and_addresses in itertools.groupby(sorted(
            (name, address)
            for address, name in bt.discover_devices(lookup_names=True)
        ), key=lambda name_and_address: name_and_address[0])
    }
    return mac_addresses_by_name


if __name__ == '__main__':
    main()
