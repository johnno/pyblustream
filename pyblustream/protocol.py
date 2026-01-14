import asyncio
import logging
import re
import time
from asyncio import Task
from typing import Any, Callable, Optional

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
        heartbeat_time=60,
        reconnect_time=10,
        use_event_connection_for_commands=False,
        wait_for_prompt=True,
        expected_prompt="Please Input Your Command :",
        enable_heartbeat=True,
    ):
        self._logger = logging.getLogger(__name__)
        self._heartbeat_time = heartbeat_time
        self._reconnect_time = reconnect_time
        self._hostname = hostname
        self._port = port
        self._source_change_callback = callback
        self._loop = asyncio.get_event_loop()
        self._use_event_connection_for_commands = use_event_connection_for_commands
        self._wait_for_prompt = wait_for_prompt
        self._expected_prompt = expected_prompt
        self._enable_heartbeat = enable_heartbeat

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
        # Command queue to serialize all outgoing commands
        self._command_queue: asyncio.Queue = asyncio.Queue()
        self._command_worker_task: Optional[Task[Any]] = None
        # Track last send time to avoid clashes between commands
        self._last_send_time: float = 0.0
        # Track last received data time to detect silent/stalled connections
        self._last_received_time: float = time.time()
        # Minimum delay between sends to avoid MCU serial port clashes (in seconds)
        self._min_send_delay: float = 1.0

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
        
        # Cancel any existing tasks before creating new ones
        if self._command_worker_task is not None and not self._command_worker_task.done():
            self._command_worker_task.cancel()
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        
        # Start command worker and heartbeat tasks
        self._command_worker_task = self._loop.create_task(self._command_worker())
        if self._enable_heartbeat:
            self._heartbeat_task = self._loop.create_task(self._heartbeat())
        
        self.send_status_message()

    async def _command_worker(self):
        """Worker task that processes all outgoing commands from the queue."""
        while True:
            try:
                message, use_persistent, response_validator = await self._command_queue.get()
                try:
                    # Enforce minimum delay between commands to avoid MCU serial port clashes
                    time_since_last_send = time.time() - self._last_send_time
                    if time_since_last_send < self._min_send_delay:
                        wait_time = self._min_send_delay - time_since_last_send
                        self._logger.debug(f"Waiting {wait_time:.2f}s before sending command")
                        await asyncio.sleep(wait_time)
                    
                    if use_persistent:
                        if self._transport and self._connected:
                            self._logger.debug(f"data_send persistent: {message.encode()}")
                            self._transport.write(message.encode())
                        else:
                            self._logger.warning("Cannot send on persistent connection: not connected")
                    else:
                        await self._send_ephemeral_internal(message, response_validator)
                    
                    self._last_send_time = time.time()
                except Exception as e:
                    self._logger.error(f"Error sending command: {e}")
                finally:
                    self._command_queue.task_done()
            except asyncio.CancelledError:
                self._logger.debug("Command worker cancelled")
                break

    async def _heartbeat(self):
        """Send keepalive only when connection has been silent to detect stalled connections."""
        while True:
            await asyncio.sleep(self._heartbeat_time)
            
            # Only send heartbeat if we haven't heard from the switch recently
            time_since_last_received = time.time() - self._last_received_time
            if time_since_last_received >= self._heartbeat_time:
                # Only send if queue is empty - other commands will keep switch awake
                if self._command_queue.empty():
                    self._logger.debug(
                        f"heartbeat - connection silent for {time_since_last_received:.1f}s, "
                        "sending OUTSTA query to check connection"
                    )
                    # Use OUTSTA command - quick output status check to verify connection
                    self._data_send_persistent("OUTSTA\r")
                else:
                    self._logger.debug(
                        f"heartbeat - connection silent for {time_since_last_received:.1f}s "
                        "but queue has pending commands, skipping"
                    )
            else:
                self._logger.debug(
                    f"heartbeat - received data {time_since_last_received:.1f}s ago, "
                    "connection is alive, skipping"
                )

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
        if self._command_worker_task is not None:
            self._command_worker_task.cancel()
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
        self._last_received_time = time.time()

        for letter in data:
            # Don't add these to the message as we don't need them.
            if letter != ord("\r") and letter != ord("\n"):
                self._received_message += chr(letter)
            if letter == ord("\n"):
                self._logger.debug(f"Whole message: {self._received_message}")
                self._process_received_packet(self._received_message)
                self._received_message = ""

    def _data_send(self, message: str, response_validator: Optional[Callable[[str], bool]] = None):
        """Queue a command to be sent (either persistent or ephemeral based on config).
        
        Args:
            message: The message to send
            response_validator: Optional function that returns True if response is valid
        """
        use_persistent = self._use_event_connection_for_commands
        try:
            self._command_queue.put_nowait((message, use_persistent, response_validator))
        except asyncio.QueueFull:
            self._logger.error("Command queue is full, dropping command")

    async def _send_ephemeral_internal(self, message, response_validator: Optional[Callable[[str], bool]] = None):
        """Send a command via ephemeral connection. Called by command worker only."""
        self._logger.debug(f"Ephemeral: Opening connection for command: {message.encode()}")
        
        try:
            # Create protocol handler for this connection to receive responses
            prompt_received = asyncio.Event()
            response_received = asyncio.Event()
            response_data = {"matched": False, "messages": [], "command_sent": False}
            
            class EphemeralProtocol(asyncio.Protocol):
                def __init__(self, prompt_received, response_received, response_data, logger, expected_prompt, validator):
                    self._prompt_received = prompt_received
                    self._response_received = response_received
                    self._response_data = response_data
                    self._logger = logger
                    self._expected_prompt = expected_prompt
                    self._validator = validator
                    self._buffer = ""
                
                def data_received(self, data):
                    self._logger.debug(f"Ephemeral: Received data: {data}")
                    for letter in data:
                        if letter != ord("\r") and letter != ord("\n"):
                            self._buffer += chr(letter)
                        if letter == ord("\n"):
                            msg = self._buffer
                            self._buffer = ""
                            self._response_data["messages"].append(msg)
                            self._logger.debug(f"Ephemeral: Parsed message: '{msg}'")
                            
                            # Check if this is the expected prompt (before command is sent)
                            if msg == self._expected_prompt and not self._response_data["command_sent"]:
                                self._logger.debug(f"Ephemeral: Expected prompt received, ready to send")
                                self._response_data["matched"] = True
                                self._prompt_received.set()
                            # After command is sent, validate response if validator provided
                            elif self._response_data["command_sent"]:
                                if self._validator:
                                    if self._validator(msg):
                                        self._logger.info(f"Ephemeral: Command succeeded: '{msg}'")
                                        self._response_data["matched"] = True
                                        self._response_received.set()
                                    else:
                                        self._logger.debug(f"Ephemeral: Unexpected response (not a success): '{msg}'")
                                else:
                                    # No validator, can't determine success/failure
                                    self._logger.debug(f"Ephemeral: Response received (no success validation configured): '{msg}'")
                                    self._response_received.set()
            
            transport, protocol = await self._loop.create_connection(
                lambda: EphemeralProtocol(prompt_received, response_received, response_data, self._logger, 
                                         self._expected_prompt, response_validator),
                host=self._hostname,
                port=self._port
            )
            self._logger.debug(f"Ephemeral: Connection established")
            
            # Wait for the prompt before sending the command (if configured)
            if self._wait_for_prompt:
                try:
                    await asyncio.wait_for(prompt_received.wait(), timeout=3.0)
                    self._logger.debug(f"Ephemeral: Prompt confirmed, proceeding to send")
                except asyncio.TimeoutError:
                    self._logger.warning(
                        f"Ephemeral: Timeout waiting for prompt. "
                        f"Received {len(response_data['messages'])} messages: {response_data['messages']}"
                    )
            
            # Send the command
            self._logger.debug(f"Ephemeral: Writing to transport: {message.encode()}")
            transport.write(message.encode())
            response_data["command_sent"] = True
            self._logger.debug(f"Ephemeral: Data written to transport")
            
            # Give a tiny delay to ensure write buffer is flushed before we start monitoring
            await asyncio.sleep(0.01)
            
            # Monitor for responses after sending command - exit early when received
            monitor_time = 2.0
            self._logger.debug(f"Ephemeral: Monitoring connection for up to {monitor_time}s for response")
            start_time = time.time()
            
            try:
                await asyncio.wait_for(response_received.wait(), timeout=monitor_time)
                elapsed = time.time() - start_time
                self._logger.debug(f"Ephemeral: Response received after {elapsed:.2f}s")
            except asyncio.TimeoutError:
                elapsed = time.time() - start_time
                self._logger.warning(f"Ephemeral: No response received after sending command (waited {elapsed:.2f}s)")
            
            # Log all messages received after sending
            messages_after_send = response_data['messages'][1:] if len(response_data['messages']) > 1 else []
            if messages_after_send:
                self._logger.debug(
                    f"Ephemeral: Received {len(messages_after_send)} message(s) after sending: {messages_after_send}"
                )
            
            self._logger.debug(f"Ephemeral: Closing connection after {elapsed:.2f}s")
            transport.close()
            self._logger.debug(f"Ephemeral: Connection closed")
            
        except Exception as e:
            self._logger.error(f"Ephemeral: Error in ephemeral connection: {e}")

    def _data_send_persistent(self, message):
        """Queue a command to be sent via the persistent connection."""
        try:
            self._command_queue.put_nowait((message, True, None))
        except asyncio.QueueFull:
            self._logger.error("Command queue is full, dropping command")

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
        # Create validator for this specific command
        def validate_response(message: str) -> bool:
            # Expect: [SUCCESS]Set output 05 connect from input 03.
            pattern = re.compile(rf".*SUCCESS.*output\s*{output_id:02d}\s.*from input\s*{input_id:02d}.*", re.IGNORECASE)
            return pattern.match(message) is not None
        
        self._data_send(f"out{output_id:02d}fr{input_id:02d}\r", validate_response)

    def send_status_message(self):
        self._logger.info(f"Sending status change message")
        # Create validator for status command - expects the Power header line
        def validate_response(message: str) -> bool:
            return FIRST_LINE.match(message) is not None
        
        self._data_send("STATUS\r", validate_response)

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
        # Create validator for power on command
        def validate_response(message: str) -> bool:
            # Expect: [SUCCESS]Set system power ON
            return re.match(r".*SUCCESS.*power\s*ON.*", message, re.IGNORECASE) is not None
        
        self._data_send(f"PON\r", validate_response)

    def send_turn_off_message(self):
        # Create validator for power off command
        def validate_response(message: str) -> bool:
            # Expect: [SUCCESS]Set system power OFF
            return re.match(r".*SUCCESS.*power\s*OFF.*", message, re.IGNORECASE) is not None
        
        self._data_send(f"POFF\r", validate_response)

    def send_guest_command(self, guest_is_input, guest_id, command):
        prefix = "IN" if guest_is_input else "OUT"
        open_command = f"{prefix}{guest_id:03}GUEST"
        close_command = "CLOSEACMGUEST"
        rn = "\r\n"
        full_command = open_command + rn + command + rn + close_command + rn
        self._data_send(full_command)

    def send_cec_power_on(self, output_id: int):
        """Send CEC power on command to output."""
        self._logger.info(f"Sending CEC power on to output {output_id}")
        self._data_send(f"OUT{output_id:02d} CEC PON\r")

    def send_cec_power_off(self, output_id: int):
        """Send CEC power off command to output."""
        self._logger.info(f"Sending CEC power off to output {output_id}")
        self._data_send(f"OUT{output_id:02d} CEC POFF\r")

    def send_cec_volume_up(self, output_id: int):
        """Send CEC volume up command to output."""
        self._logger.info(f"Sending CEC volume up to output {output_id}")
        self._data_send(f"OUT{output_id:02d} CEC VOLUP\r")

    def send_cec_volume_down(self, output_id: int):
        """Send CEC volume down command to output."""
        self._logger.info(f"Sending CEC volume down to output {output_id}")
        self._data_send(f"OUT{output_id:02d} CEC VOLDOWN\r")

    def send_cec_mute(self, output_id: int):
        """Send CEC mute toggle command to output."""
        self._logger.info(f"Sending CEC mute toggle to output {output_id}")
        self._data_send(f"OUT{output_id:02d} CEC MUTE\r")
