import asyncio
import unittest
from unittest.mock import MagicMock, AsyncMock

from pyblustream.protocol import MatrixProtocol


class TestMatrixProtocol(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.mock_callback = MagicMock()

    def test_send_command_ephemeral(self):
        async def test_logic():
            # Mock server
            server = await asyncio.start_server(lambda r, w: None, '127.0.0.1', 0)
            port = server.sockets[0].getsockname()[1]

            protocol = MatrixProtocol(
                '127.0.0.1',
                port,
                self.mock_callback,
                use_event_connection_for_commands=False
            )
            protocol._loop = self.loop

            # Mock the persistent connection
            protocol._transport = MagicMock()
            protocol._connected = True

            # Use a future to capture the ephemeral connection
            ephemeral_connection_future = self.loop.create_future()

            async def handle_connection(reader, writer):
                ephemeral_connection_future.set_result(writer)

            server.set_serving_callback(handle_connection)

            protocol.send_change_source(1, 1)

            try:
                writer = await asyncio.wait_for(ephemeral_connection_future, timeout=1)
                self.assertIsNotNone(writer)
                writer.close()
                await writer.wait_closed()
            except asyncio.TimeoutError:
                self.fail("Ephemeral connection not received")
            finally:
                server.close()
                await server.wait_closed()

        self.loop.run_until_complete(test_logic())

    def test_send_command_persistent(self):
        async def test_logic():
            protocol = MatrixProtocol(
                '127.0.0.1',
                12345,
                self.mock_callback,
                use_event_connection_for_commands=True
            )
            protocol._loop = self.loop
            mock_transport = MagicMock()
            protocol._transport = mock_transport
            protocol._connected = True

            protocol.send_change_source(1, 1)

            mock_transport.write.assert_called_with(b'out01fr01\r')

        self.loop.run_until_complete(test_logic())


if __name__ == '__main__':
    unittest.main()

