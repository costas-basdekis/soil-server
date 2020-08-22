#!/usr/bin/env python3
import datetime
import itertools
import re
import time

import bluetooth as bt

import bluetoothctl

BLUETOOTH_PATTERN = re.compile(r'^SOIL-\d+$')
SLEEP_TIME = datetime.timedelta(seconds=1)
SEARCH_INTERVAL = datetime.timedelta(seconds=10)

BT_ERROR_NO_SUCH_DEVICE = 19
BT_ERROR_DISCONNECTED = 107
BT_ERROR_HOST_IS_DOWN = 112


class DeviceServer:
    def __init__(self, retries=1):
        self.devices = Devices(retries=retries)

    @classmethod
    def start_new_server(cls, retries=1):
        server = cls(retries=retries)
        server.loop()

    def loop(self):
        with self.devices:
            while True:
                self.devices.find_and_connect()
                time_since_last_search = datetime.timedelta(seconds=0)
                while time_since_last_search < SEARCH_INTERVAL:
                    self.receive_and_handle_data()
                    time.sleep(SLEEP_TIME.total_seconds())
                    time_since_last_search += SLEEP_TIME

    def receive_and_handle_data(self):
        data = self.devices.receive_data()
        self.devices.print_data(data)


class Devices:
    def __init__(self, retries=1):
        self.devices = set()
        self.by_socket = {}
        self.by_address = {}
        self.by_name = {}
        self.retries = retries
        self.discovery = BluetoothDiscovery()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_all()

    def add(self, device):
        self.devices.add(device)
        self.by_socket[device.socket] = device
        self.by_address[device.address] = device
        self.by_name.setdefault(device.name, set()).add(device)

    def remove(self, device):
        self.devices.remove(device)
        del self.by_socket[device.socket]
        del self.by_address[device.address]
        self.by_name[device.name].remove(device)

    def close(self, device):
        if device.connected:
            device.close()
        self.remove(device)

    def close_all(self):
        for device in self.devices:
            self.close(device)

    def find_and_connect(self, pattern=BLUETOOTH_PATTERN):
        self.discovery.find_and_connect(
            self, pattern=pattern, retries=self.retries)

    def receive_data(self):
        data = {
            device: device.receive_data()
            for device in self.devices
            if device.connected
        }
        disconnected_devices = {
            device
            for device in self.devices
            if not device.connected
        }
        for device in disconnected_devices:
            self.remove(device)

        return data

    def print_data(self, data):
        for device, lines in data.items():
            device.print_data(lines)


class Device:
    def __init__(self, socket, address, name, buffer=b""):
        self.socket = socket
        self.address = address
        self.name = name
        self.buffer = buffer
        self.connected = True

    @classmethod
    def create(cls, address, name):
        socket = bt.BluetoothSocket(bt.RFCOMM)
        try:
            socket.connect((address, 1))
        except bt.BluetoothError as e:
            if e.errno == BT_ERROR_HOST_IS_DOWN:
                error = "Could not connect to {}: host is down".format(address)
                return None, error
            raise
        socket.setblocking(False)
        return cls(socket, address, name), None

    def receive_data(self,):
        new_data = self.get_new_data()
        if not new_data:
            self.close_if_closed()
            return []
        new_lines = self.append_socket_data(new_data)

        return new_lines

    def print_data(self, lines):
        short_address = self.address[-2:]
        for line in lines:
            print(">{}: {}".format(short_address, line))

    def get_new_data(self):
        new_data = b""
        while True:
            try:
                new_data += self.socket.recv(1024)
            except bt.BluetoothError:
                break
        return new_data

    def append_socket_data(self, new_data):
        self.buffer += new_data
        *new_lines, self.buffer = self.buffer.split(b'\r\n')
        return list(map(bytes.decode, new_lines))

    def close_if_closed(self):
        if self.is_socket_still_connected():
            return
        print("{} disconnected".format(self.address))

    def is_socket_still_connected(self):
        try:
            self.socket.getpeername()
            return True
        except bt.BluetoothError as e:
            if e.errno == BT_ERROR_DISCONNECTED:
                return False
            raise

    def close(self):
        self.socket.close()
        self.connected = False
        self.buffer = None


class BluetoothDiscovery:
    def __init__(self):
        self.bctl = bluetoothctl.Bluetoothctl()
        self.bctl.start_scan()

    def find_and_connect(self, devices, pattern=BLUETOOTH_PATTERN, retries=1):
        print("Finding devices")
        mac_addresses_by_name = self.get_mac_addresses_by_name()
        soil_addresses_and_names = sum((
            [(address, name) for address in addresses]
            for name, addresses in mac_addresses_by_name.items()
            if pattern.match(name)
        ), [])
        print("Got {} names, {} matching {}".format(
            len(mac_addresses_by_name), len(soil_addresses_and_names),
            pattern.pattern))
        connections_created = self.create_connections(
            devices, soil_addresses_and_names, retries=retries)
        print("Connected to {} devices".format(connections_created))

    def get_mac_addresses_by_name(self):
        return self.get_mac_addresses_by_name_with_bctl()

    def get_mac_addresses_by_name_with_bctl(self):
        def sort_by_name(device):
            return device['name']
        devices = self.bctl.get_paired_devices()
        mac_addresses_by_name = {
            name: [device['mac_address'] for device in grouped_devices]
            for name, grouped_devices
            in itertools.groupby(
                sorted(devices, key=sort_by_name), key=sort_by_name)
        }
        return mac_addresses_by_name

    def get_mac_addresses_by_name_with_bluez(self):
        try:
            devices = bt.discover_devices(
                duration=8, lookup_names=True, flush_cache=True)
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

    def create_connections(self, devices, soil_addresses_and_names, retries=1):
        connections_created = 0
        for address, name in soil_addresses_and_names:
            for retry in range(retries):
                if self.create_connection(devices, address, name):
                    connections_created += 1
                    break

        return connections_created

    def create_connection(self, devices, address, name):
        device, error = Device.create(address, name)
        if error:
            print(error)
            return False
        print("Connected to {}".format(device.address))
        devices.add(device)

        return True


if __name__ == '__main__':
    DeviceServer.start_new_server(retries=5)
