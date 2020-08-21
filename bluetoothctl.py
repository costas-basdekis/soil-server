# ReachView code is placed under the GPL license.
# Written by Egor Fedorov (egor.fedorov@emlid.com)
# Copyright (c) 2015, Emlid Limited
# All rights reserved.

# If you are interested in using ReachView code as a part of a
# closed source project, please contact Emlid Limited (info@emlid.com).

# This file is part of ReachView.

# ReachView is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# ReachView is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with ReachView.  If not, see <http://www.gnu.org/licenses/>.

import time
import pexpect
import subprocess


class BluetoothctlError(Exception):
    """This exception is raised, when bluetoothctl fails to start."""
    pass


class Bluetoothctl:
    """A wrapper for bluetoothctl utility."""

    def __init__(self):
        subprocess.check_output("rfkill unblock bluetooth", shell=True)
        self.child = pexpect.spawn("bluetoothctl", echo=False)

    def get_output(self, command, pause=0):
        """Run a command in bluetoothctl prompt, return output as a list of lines."""
        self.child.send(command + "\n")
        time.sleep(pause)
        start_failed = self.child.expect(["bluetooth", pexpect.EOF])

        if start_failed:
            raise BluetoothctlError("Bluetoothctl failed after running " + command.decode())

        return self.child.before.decode().split("\r\n")

    def start_scan(self):
        """Start bluetooth scanning process."""
        try:
            return self.get_output("scan on")
        except BluetoothctlError as e:
            print(e)
            return None

    def make_discoverable(self):
        """Make device discoverable."""
        try:
            return self.get_output("discoverable on")
        except BluetoothctlError as e:
            print(e)
            return None

    def parse_device_info(self, info_string):
        """Parse a string corresponding to a device."""
        device = {}
        block_list = ["[\x1b[0;", "removed"]
        string_valid = not any(keyword in info_string for keyword in block_list)

        if string_valid:
            try:
                device_position = info_string.index("Device")
            except ValueError:
                pass
            else:
                if device_position > -1:
                    attribute_list = info_string[device_position:].split(" ", 2)
                    device = {
                        "mac_address": attribute_list[1],
                        "name": attribute_list[2],
                    }

        return device

    def get_available_devices(self):
        """Return a list of tuples of paired and discoverable devices."""
        try:
            out = self.get_output("devices")
        except BluetoothctlError as e:
            print(e)
            return None

        available_devices = list(filter(None, (
            self.parse_device_info(line)
            for line in out
        )))

        return available_devices

    def get_paired_devices(self):
        """Return a list of tuples of paired devices."""
        try:
            out = self.get_output("paired-devices")
        except BluetoothctlError as e:
            print(e)
            return None

        paired_devices = list(filter(None, (
            self.parse_device_info(line)
            for line in out
        )))

        return paired_devices

    def get_discoverable_devices(self):
        """Filter paired devices out of available."""
        available = self.get_available_devices()
        paired = self.get_paired_devices()

        return list(set(available) - set(paired))

    def get_device_info(self, mac_address):
        """Get device info by mac address."""
        try:
            return  self.get_output("info " + mac_address)
        except BluetoothctlError as e:
            print(e)
            return None

    def pair(self, mac_address):
        """Try to pair with a device by mac address."""
        try:
            self.get_output("pair " + mac_address, 4)
        except BluetoothctlError as e:
            print(e)
            return None

        result = self.child.expect(["Failed to pair", "Pairing successful", pexpect.EOF])
        success = result == 1
        return success

    def remove(self, mac_address):
        """Remove paired device by mac address, return success of the operation."""
        try:
            self.get_output("remove " + mac_address, 3)
        except BluetoothctlError as e:
            print(e)
            return None

        result = self.child.expect(["not available", "Device has been removed", pexpect.EOF])
        success = result == 1
        return success

    def connect(self, mac_address):
        """Try to connect to a device by mac address."""
        try:
            self.get_output("connect " + mac_address, 2)
        except BluetoothctlError as e:
            print(e)
            return None

        result = self.child.expect(["Failed to connect", "Connection successful", pexpect.EOF])
        success = result == 1
        return success

    def disconnect(self, mac_address):
        """Try to disconnect to a device by mac address."""
        try:
            self.get_output("disconnect " + mac_address, 2)
        except BluetoothctlError as e:
            print(e)
            return None

        result = self.child.expect(["Failed to disconnect", "Successful disconnected", pexpect.EOF])
        success = result == 1
        return success
