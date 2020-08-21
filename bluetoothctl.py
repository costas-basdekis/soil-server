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
import re
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
        self.child.send("{}\n".format(command))
        time.sleep(pause)
        start_failed = self.child.expect(["bluetooth", pexpect.EOF])

        if start_failed:
            print("Error while running {}".format(command))
            return None

        return self.child.before.decode().split("\r\n")

    def start_scan(self):
        """Start bluetooth scanning process."""
        self.get_output("scan on")

    def make_discoverable(self):
        """Make device discoverable."""
        return self.get_output("discoverable on")

    def parse_device_infos(self, lines):
        if not lines:
            return []

        devices = list(filter(None, (
            self.parse_device_info(line)
            for line in lines
        )))

        return devices

    def parse_device_info(self, info_string):
        """Parse a string corresponding to a device."""
        block_list = ["[\x1b[0;", "removed"]
        string_valid = not any(keyword in info_string for keyword in block_list)

        if not string_valid:
            return None
        if "Device" not in info_string:
            return None
        _, mac_address, name = info_string\
            .split("Device", 1)[-1]\
            .split(" ", 2)
        device = {
            "mac_address": mac_address,
            "name": name,
        }

        return device

    def get_available_devices(self):
        """Return a list of tuples of paired and discoverable devices."""
        lines = self.get_output("devices")
        return self.parse_device_infos(lines)

    def get_paired_devices(self):
        """Return a list of tuples of paired devices."""
        lines = self.get_output("paired-devices")
        return self.parse_device_infos(lines)

    def get_discoverable_devices(self):
        """Filter paired devices out of available."""
        available = self.get_available_devices()
        paired = self.get_paired_devices()

        return list(set(available) - set(paired))

    RE_DEVICE_INFO = re.compile(r'\t(\w+): (.*)')

    def get_device_info(self, mac_address):
        """Get device info by mac address."""
        lines = self.get_output("info {}".format(mac_address))
        return dict(
            match.groups()
            for match in map(self.RE_DEVICE_INFO.match, lines)
            if match
        )

    def pair(self, mac_address):
        """Try to pair with a device by mac address."""
        if not self.get_output("pair {}".format(mac_address), 4):
            return False

        result = self.child.expect(["Failed to pair", "Pairing successful", pexpect.EOF])
        success = result == 1
        return success

    def remove(self, mac_address):
        """Remove paired device by mac address, return success of the operation."""
        if not self.get_output("remove {}".format(mac_address), 3):
            return False

        result = self.child.expect(["not available", "Device has been removed", pexpect.EOF])
        success = result == 1
        return success

    def connect(self, mac_address):
        """Try to connect to a device by mac address."""
        if not self.get_output("connect {}".format(mac_address), 2):
            return False

        result = self.child.expect(["Failed to connect", "Connection successful", pexpect.EOF])
        success = result == 1
        return success

    def disconnect(self, mac_address):
        """Try to disconnect to a device by mac address."""
        if not self.get_output("disconnect {}".format(mac_address), 2):
            return None

        result = self.child.expect(["Failed to disconnect", "Successful disconnected", pexpect.EOF])
        success = result == 1
        return success
