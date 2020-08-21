#!/usr/bin/env python3
import datetime
import itertools
import re
import time

import bluetooth as bt

import bluetoothctl

BLUETOOTH_PATTERN = re.compile(r'^SOIL-\d+$')
SLEEP_TIME = datetime.timedelta(seconds=1)
SEARCH_INTERVAL = datetime.timedelta(minutes=1)

BT_ERROR_NO_SUCH_DEVICE = 19
BT_ERROR_DISCONNECTED = 107
BT_ERROR_HOST_IS_DOWN = 112


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
    soil_addresses = sum((
        addresses
        for name, addresses in mac_addresses_by_name.items()
        if BLUETOOTH_PATTERN.match(name)
    ), [])
    print("Got {} names, {} matching {}".format(
        len(mac_addresses_by_name), len(soil_addresses), BLUETOOTH_PATTERN.pattern))
    for address in soil_addresses:
        socket_by_address.setdefault(address, None)
    create_connections(socket_by_address, buffer_by_socket)
    print("Connected to {} devices".format(len(socket_by_address.values())))


def is_socket_still_connected(socket):
    try:
        socket.getpeername()
        return True
    except bt.BluetoothError as e:
        if e.errno == BT_ERROR_DISCONNECTED:
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


def create_connections(socket_by_address, buffer_by_socket):
    for address, socket in list(socket_by_address.items()):
        if not socket:
            socket = socket_by_address[address] = bt.BluetoothSocket(bt.RFCOMM)
            try:
                socket.connect((address, 1))
            except bt.BluetoothError as e:
                del socket_by_address[address]
                if e.errno == BT_ERROR_HOST_IS_DOWN:
                    print("Could not connect to {}: host is down".format(address))
                    continue
                raise
            socket.setblocking(False)
            buffer_by_socket.setdefault(socket, b"")
            print("Connected to {}".format(address))


def get_mac_addresses_by_name():
    return get_mac_addresses_by_name_with_bctl()


def get_mac_addresses_by_name_with_bctl():
    bctl = bluetoothctl.Bluetoothctl()
    bctl.start_scan()
    devices = bctl.get_paired_devices()
    mac_addresses_by_name = {
        name: [device['mac_address'] for device in grouped_devices]
        for name, grouped_devices
        in itertools.groupby(sorted(devices, key=lambda device: device['name']), key=lambda device: device['name'])
    }
    return mac_addresses_by_name


def get_mac_addresses_by_name_with_bluez():
    try:
        devices = bt.discover_devices(duration=8, lookup_names=True, flush_cache=True)
    except bt.BluetoothError as e:
        if e.errno == BT_ERROR_NO_SUCH_DEVICE:
            print("Warning: Bluetooth is turned off")
            return {}
        raise
    mac_addresses_by_name = {
        name: [address for _, address in names_and_addresses]
        for name, names_and_addresses in itertools.groupby(sorted(
            (name, address)
            for address, name in devices
        ), key=lambda name_and_address: name_and_address[0])
    }
    return mac_addresses_by_name


if __name__ == '__main__':
    main()
