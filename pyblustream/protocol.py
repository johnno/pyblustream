import asyncio
import logging
import re
import time
from asyncio import Task
from typing import Any, Optional

from pyblustream.listener import SourceChangeListener

# Success message after output has changed:
# [SUCCESS]Set output 05 connect from input 03.
SUCCESS_CHANGE = re.compile(
    r".*SUCCESS.*output\s*(\d+)\s(connect )?from input\s*(\d+).*"
)
# Success message after all outputs have changed:
# [SUCCESS]Set all output connect from input 03.
SUCCESS_ALL_CHANGE = re.compile(
    r".*SUCCESS.*all output\s(connect )?from input\s*(\d+).*"
)
# Success message after all outputs have changed to to match the input port number:
# # [SUCCESS]Set output port to port.
SUCCESS_PTP = re.compile(
    r".*SUCCESS.*output port to port.*"
)
# Success System powered off:
# [SUCCESS]Set system power OFF
# Success system powered on:
# [SUCCESS]Set system power ON, please wait a moment... Done
SUCCESS_POWER = re.compile(r".*SUCCESS.*Set system power\s*(\w+).*")

# Sent from app comes like this:
# EL-4KPM-V88> out06fr02
# OUTPUT_CHANGE = re.compile('.*(?:OUT|out)\\s*([0-9]+)\\s*(?:FR|fr)\\s*([0-9]+).*', re.IGNORECASE)
OUTPUT_CHANGE_REQUESTED = re.compile(r".*OUT\s*(\d+)\s*FR\s*(\d+).*", re.IGNORECASE)

# The line in a status message that shows the input/output status:
# '01	     01		  No /Yes	  Yes/SRC   	HDBT      ON '
INPUT_STATUS_LINE = re.compile(r"(\d\d)\s+(\d\d)[^:].*")
# The line in a status message that shows the  system power status:
# Power	IR	Key	Beep	LCD
# ON 	ON 	ON 	OFF	ON
FIRST_LINE = re.compile(r".*Power\s+IR\s+Key\s+Beep\s+LCD.*")
POWER_STATUS_LINE = re.compile(
    r".*(ON|OFF)\s+(ON|OFF)\s+(ON|OFF)\s+(ON|OFF)\s+(ON|OFF).*"
)

# Received if you try to change inputs using the app when the matrix is OFF:
# From the app: out04fr08
# [ERROR]System is power off, please turn it on first.
ERROR_OFF = re.compile(r".*ERROR.*System is power off, please turn it on first.*")


class MatrixProtocol(asyncio.Protocol):
    _received_message: str
    _heartbeat_task: Optional[Task[Any]]
    _connected: bool
    _output_to_input_map: dict[int, int]
    _source_change_callback: SourceChangeListener

    def __init__(
        self,
        hostname,
        port,
        callback: SourceChangeListener,
        heartbeat_time=5,
        reconnect_time=10,
        use_event_connection_for_commands=False,
    ):
        self._logger = logging.getLogger(__name__)
        self._heartbeat_time = heartbeat_time
        self._reconnect_time = reconnect_time
        self._hostname = hostname
        self._port = port
        self._source_change_callback = callback
        self._loop = asyncio.get_event_loop()
        self._use_event_connection_for_commands = use_event_connection_for_commands

        self._connected = False
        self._reconnect = True
        self._transport = None
        self.peer_name = None
        self._received_message = ""
        self._output_to_input_map = {}
        self._matrix_on = False
        # Number of inputs on the matrix; discovered later and set via set_input_count
        self._input_count: int = 0
        self._heartbeat_task = None
        # Track last send time to avoid clashes between heartbeat and commands
        self._last_send_time: float = 0.0
        # Minimum delay between sends to avoid MCU serial port clashes (in seconds)
        self._min_send_delay: float = 0.5

    def set_input_count(self, count: int) -> None:
        """
        Set the number of inputs known for the matrix.
        Call this once Matrix has parsed metadata.
        """
        try:
            self._input_count = int(count)
        except (TypeError, ValueError):
            # keep previous value if invalid input provided
            self._logger.warning("Invalid input count provided to set_input_count")

    def connect(self):
        connection_task = self._loop.create_connection(
            lambda: self, host=self._hostname, port=self._port
        )
        self._loop.create_task(connection_task)

    async def async_connect(self):
        transport, protocol = await self._loop.create_connection(
            lambda: self, host=self._hostname, port=self._port
        )

    def close(self):
        self._reconnect = False
        if self._transport:
            self._transport.close()

    def connection_made(self, transport):
        """Method from asyncio.Protocol"""
        self._connected = True
        self._transport = transport
        self.peer_name = transport.get_extra_info("peername")
        self._logger.info(f"Connection Made: {self.peer_name}")
        self._logger.info("Requesting current status")
        self._source_change_callback.connected()
        self.send_status_message()
        # Cancel any existing heartbeat task before creating a new one
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        self._heartbeat_task = self._loop.create_task(self._heartbeat())

    async def _heartbeat(self):
        while True:
            await asyncio.sleep(self._heartbeat_time)
            # Skip heartbeat if a command was sent recently to avoid MCU serial port clash
            time_since_last_send = time.time() - self._last_send_time
            if time_since_last_send < self._heartbeat_time:
                self._logger.debug(f"Skipping heartbeat - command sent {time_since_last_send:.2f}s ago")
                continue
            self._logger.debug("heartbeat")
            self._data_send_persistent("\n")

    async def _wait_to_reconnect(self):
        # TODO with the new async_connect I think we can make this much easier - but I can't test right now
        # so not changing yet:
        # await self.async_connect()
        # using old sync connect.
        while not self._connected and self._reconnect:
            await asyncio.sleep(self._reconnect_time)
            self.connect()

    def connection_lost(self, exc):
        """Method from asyncio.Protocol"""
        self._connected = False
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
        disconnected_message = f"Disconnected from {self._hostname}"
        if self._reconnect:
            disconnected_message = (
                disconnected_message
                + " will try to reconnect in {self._reconnect_time} seconds"
            )
            self._logger.error(disconnected_message)
        else:
            disconnected_message = disconnected_message + " not reconnecting"
            # Only info in here as close has been called.
            self._logger.info(disconnected_message)
        self._source_change_callback.disconnected()
        if self._reconnect:
            self._loop.create_task(self._wait_to_reconnect())
        pass

    def data_received(self, data):
        """Method from asyncio.Protocol"""
        self._logger.debug(f"data_received client: {data}")

        for letter in data:
            # Don't add these to the message as we don't need them.
            if letter != ord("\r") and letter != ord("\n"):
                self._received_message += chr(letter)
            if letter == ord("\n"):
                self._logger.debug(f"Whole message: {self._received_message}")
                self._process_received_packet(self._received_message)
                self._received_message = ""

    def _data_send(self, message):
        if self._use_event_connection_for_commands:
            self._data_send_persistent(message)
        else:
            self._loop.create_task(self._data_send_ephemeral(message))

    async def _data_send_ephemeral(self, message):
        # Wait if heartbeat was just sent to avoid MCU serial port clash
        time_since_last_send = time.time() - self._last_send_time
        if time_since_last_send < self._min_send_delay:
            wait_time = self._min_send_delay - time_since_last_send
            self._logger.debug(f"Waiting {wait_time:.2f}s before sending command")
            await asyncio.sleep(wait_time)
        
        self._logger.debug(f"data_send_ephemeral client: {message.encode()}")
        try:
            transport, _ = await self._loop.create_connection(
                lambda: asyncio.Protocol(), host=self._hostname, port=self._port
            )
            transport.write(message.encode())
            self._last_send_time = time.time()
            transport.close()
        except Exception as e:
            self._logger.error(f"Error in ephemeral connection: {e}")

    def _data_send_persistent(self, message):
        self._logger.debug(f"data_send client: {message.encode()}")
        self._transport.write(message.encode())
        self._last_send_time = time.time()

    # noinspection DuplicatedCode
    def _process_received_packet(self, message):
        # Message received in response to anyone changing the source.
        success_change_match = SUCCESS_CHANGE.match(message)
        if success_change_match:
            self._logger.debug(f"Input change message received: {message}")
            output_id = success_change_match.group(1)
            input_id = success_change_match.group(3)
            self._process_input_changed(input_id, output_id)
            return

        # Message received in response to anyone changing the source on all outputs.
        success_all_change_match = SUCCESS_ALL_CHANGE.match(message)
        if success_all_change_match:
            self._logger.debug(f"Input change on all outputs message received: {message}")
            input_id = success_all_change_match.group(2)
            outputs = self._output_to_input_map.keys()
            for output_id in outputs:
                self._process_input_changed(input_id, output_id)
            return

        # Message received in response to anyone changing the source on all outputs to match the input port number.
        success_ptp_match = SUCCESS_PTP.match(message)
        if success_ptp_match:
            self._logger.debug(f"PTP Input change message received: {message}")
            loop_count = min(self._input_count, len(self._output_to_input_map))
            for output_id in range(1, loop_count + 1):
                self._process_input_changed(output_id, output_id)
            return

        # Message received in response to anyone changing the power on/off
        success_power_match = SUCCESS_POWER.match(message)
        if success_power_match:
            self._logger.debug(f"Power change message received: {message}")
            power = success_power_match.group(1)
            self._process_power_changed(power)
            return

        # Someone has sent a message to change the source - this is just for info we don't need to do anything on this.
        output_change_match = OUTPUT_CHANGE_REQUESTED.match(message)
        if output_change_match:
            self._logger.debug(f"Input change message received: {message}")
            output_id = int(output_change_match.group(1))
            input_id = int(output_change_match.group(2))
            self._source_change_callback.source_change_requested(output_id, input_id)
            return

        # Error received when someone tries to change a source whilst the matrix is powered off.
        error_match = ERROR_OFF.match(message)
        if error_match:
            self._logger.info(f"Error message received: {message}")
            self._source_change_callback.error(message)

        # Lines that show inputs/outputs when someone has called STATUS
        input_status_change_match = INPUT_STATUS_LINE.match(message)
        if input_status_change_match:
            self._logger.debug(f"Status Input change message received: {message}")
            output_id = input_status_change_match.group(1)
            input_id = input_status_change_match.group(2)
            self._process_input_changed(input_id, output_id)
            return

        # Lines that show inputs/outputs when someone has called STATUS
        power_status_change_match = POWER_STATUS_LINE.match(message)
        if power_status_change_match:
            self._logger.debug(f"Status Power message received: {message}")
            power = power_status_change_match.group(1)
            self._process_power_changed(power)
            return

        self._logger.debug(f"Not an input change message received: {message}")

    def _process_input_changed(self, input_id, output_id):
        self._logger.debug(f"Input ID [{input_id}] Output id [{output_id}]")
        input_id_int = int(input_id)
        output_id_int = int(output_id)
        self._output_to_input_map[output_id_int] = input_id_int
        self._source_change_callback.source_changed(output_id_int, input_id_int)

    def _process_power_changed(self, power):
        self._logger.debug(f"Power change to [{power}]")
        self._matrix_on = power == "ON"  # Otherwise it is OFF
        self._source_change_callback.power_changed(power)

    def send_change_source(self, input_id: int, output_id: int):
        self._logger.info(
            f"Sending Output source change message - Output: {output_id} changed to input: {input_id}"
        )
        self._data_send(f"out{output_id:02d}fr{input_id:02d}\r")

    def send_status_message(self):
        self._logger.info(f"Sending status change message")
        self._data_send("STATUS\r")

    def get_status_of_output(self, output_id: int) -> Optional[int]:
        return self._output_to_input_map.get(output_id, None)

    def get_status_of_all_outputs(self) -> list[tuple[int, Optional[int]]]:
        return_list: list[tuple[int, int | None]] = []
        for output_id in self._output_to_input_map:
            input_id = self._output_to_input_map.get(output_id, None)
            return_list.append((output_id, input_id))
        return return_list

    def is_matrix_on(self) -> bool:
        return self._matrix_on

    def send_turn_on_message(self):
        self._data_send(f"PON\r")

    def send_turn_off_message(self):
        self._data_send(f"POFF\r")

    def send_guest_command(self, guest_is_input, guest_id, command):
        prefix = "IN" if guest_is_input else "OUT"
        open_command = f"{prefix}{guest_id:03}GUEST"
        close_command = "CLOSEACMGUEST"
        rn = "\r\n"
        full_command = open_command + rn + command + rn + close_command + rn
        self._data_send(full_command)
