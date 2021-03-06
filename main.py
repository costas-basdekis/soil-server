#!/usr/bin/env python3
import argparse
import datetime
import itertools
import json
import queue
import re
import threading
import time
from urllib.parse import urljoin

import bluetooth as bt
import requests

import bluetoothctl

BT_ERROR_NO_SUCH_DEVICE = 19
BT_ERROR_DISCONNECTED = 107
BT_ERROR_HOST_IS_DOWN = 112


class DeviceServer:
    SLEEP_TIME = datetime.timedelta(seconds=1)
    SEARCH_INTERVAL = datetime.timedelta(seconds=10)
    DEFAULT_API_URL = "http://localhost:8000/api/"
    MEASUREMENTS_API_PATH = "measurements/"

    def __init__(self, retries=1, api_url=None):
        self.devices = Devices(retries=retries)
        self.discovery_thread = threading.Thread(
            target=self.threaded_find_and_connect, daemon=True)
        self.should_be_discovering = False
        self.new_devices = queue.Queue()
        if api_url is None:
            self.api = self.DEFAULT_API_URL
        else:
            self.api = api_url
        self.measurements_api_url = \
            urljoin(f"{api_url}/", self.MEASUREMENTS_API_PATH)
        self.devices_best_startup_estimates = {}

    @classmethod
    def start_new_server(cls, retries=1, api_url=None):
        server = cls(retries=retries, api_url=api_url)
        server.loop()

    def loop(self):
        self.should_be_discovering = True
        if not self.discovery_thread.is_alive():
            self.discovery_thread.start()
        try:
            with self.devices:
                while True:
                    self.receive_and_handle_data()
                    self.add_new_devices()
        finally:
            self.should_be_discovering = False

    def threaded_find_and_connect(self):
        while self.should_be_discovering:
            try:
                new_devices = self.devices.find_and_connect(False)
                for device in new_devices:
                    self.new_devices.put(device)
            except Exception as e:
                print("Error while discovering: {}".format(e))
            time.sleep(self.SEARCH_INTERVAL.total_seconds())

    def add_new_devices(self):
        while not self.new_devices.empty():
            device = self.new_devices.get(block=False)
            self.devices.add(device)

    def receive_and_handle_data(self):
        data = self.devices.receive_data()
        self.devices.print_data(data)
        try:
            measurements = self.parse_data(data)
            self.log_measurements(measurements)
        except Exception as e:
            print("Error while parsing and logging measurements: {}".format(e))

    def parse_data(self, data):
        measurements = []
        for device, (lines, received_at) in data.items():
            for line in lines:
                error, device_measurements = self.parse_device_line(
                    device, line, received_at=received_at)
                if error:
                    print(error)
                    continue
                measurements.extend(device_measurements)

        return measurements

    RE_PARSE_LINE = re.compile(r'^\[(\d+)\[(.*)]\1]$')

    def parse_device_line(self, device, line, received_at=None):
        # [100[{"controller_id": 100, "measurements":[{"sensor_id": 1,
        # "plant_id": 1, "moisture": 70}], "flow": 7991, "millis": 1109147}]100]
        match = self.RE_PARSE_LINE.match(line)
        if not match:
            return "Could not match line", None

        data_json = match.group(2)
        try:
            data = json.loads(data_json)
        except Exception as e:
            return "Could not parse JSON: {}".format(e), None

        if received_at is None:
            received_at = datetime.datetime.now()
        received_at_str = received_at.isoformat()
        device_name = device.name
        try:
            raw_measurements = data['measurements']
            controller_id = data['controller_id']
            time_since_startup = datetime.timedelta(milliseconds=data['millis'])
            device_startup_estimate = received_at - time_since_startup
            device_best_startup_estimate = \
                self.get_device_best_startup_estimate(
                    data['controller_id'], device_startup_estimate)
            taken_at = device_best_startup_estimate + time_since_startup

            measurements = [
                {
                    "controller_id": controller_id,
                    "sensor_id": raw_measurement['sensor_id'],
                    "bluetooth_name": device_name,
                    "plant_id": raw_measurement['plant_id'],
                    "moisture": raw_measurement['moisture'],
                    "received_at": received_at_str,
                    "taken_at": taken_at,
                }
                for raw_measurement in raw_measurements
            ]
        except Exception as e:
            return "Could not parse data: {}".format(e), None

        return None, measurements

    def log_measurements(self, measurements):
        for measurement in measurements:
            response = requests.post(
                self.measurements_api_url, data=measurement)
            if not response.ok:
                print("Error {} logging with data {}: {}".format(
                    response.status_code, json.dumps(measurement),
                    response.text))

    def get_device_best_startup_estimate(self, controller_id,
                                         device_startup_estimate):
        current_device_best_startup_estimate = \
            self.devices_best_startup_estimates\
            .setdefault(controller_id, device_startup_estimate)
        self.devices_best_startup_estimates[controller_id] = min(
            current_device_best_startup_estimate, device_startup_estimate)

        return self.devices_best_startup_estimates[controller_id]


class Devices:
    BLUETOOTH_PATTERN = re.compile(r'^SOIL-\d+$')

    def __init__(self, retries=1, pattern=BLUETOOTH_PATTERN):
        self.devices = set()
        self.by_socket = {}
        self.by_address = {}
        self.by_name = {}
        self.retries = retries
        self.pattern = pattern
        self.discovery = BluetoothDiscovery()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_all()

    def add_many(self, devices):
        for device in devices:
            self.add(device)

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
        for device in list(self.devices):
            self.close(device)

    def find_and_connect(self, add=True):
        new_devices = self.discovery.find_and_connect(
            pattern=self.pattern,
            ignore_addresses=self.get_connected_addresses(),
            retries=self.retries,
        )

        if add:
            self.add_many(new_devices)

        return new_devices

    def get_connected_addresses(self):
        return [
            device.address
            for device in self.devices
            if device.connected
        ]

    def receive_data(self):
        data = {
            device: (device.receive_data(), datetime.datetime.now())
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
        for device, (lines, _) in data.items():
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
        self.close()

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

    def find_and_connect(self, pattern, ignore_addresses=(), retries=1):
        print("Finding devices")
        mac_addresses_by_name = self.get_mac_addresses_by_name()
        soil_addresses_and_names = sum((
            [
                (address, name)
                for address in addresses
                if address not in ignore_addresses
            ]
            for name, addresses in mac_addresses_by_name.items()
            if pattern.match(name)
        ), [])
        print("Got {} names, {} matching {}".format(
            len(mac_addresses_by_name), len(soil_addresses_and_names),
            pattern.pattern))
        new_devices = self.create_connections(
            soil_addresses_and_names, retries=retries)
        print("Connected to {} new devices".format(len(new_devices)))

        return new_devices

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

    def create_connections(self, soil_addresses_and_names, retries=1):
        new_devices = []
        for address, name in soil_addresses_and_names:
            for retry in range(retries):
                device = self.create_connection(address, name)
                if device:
                    new_devices.append(device)
                    break

        return new_devices

    def create_connection(self, address, name):
        device, error = Device.create(address, name)
        if error:
            print(error)
            return None
        print("Connected to {}".format(device.address))

        return device


def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument(
        '--api-url', help='The API base URL',
        default=DeviceServer.DEFAULT_API_URL)
    args = parser.parse_args()
    DeviceServer.start_new_server(retries=5, api_url=args.api_url)


if __name__ == '__main__':
    main()
