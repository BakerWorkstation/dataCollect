#syncio!/usr/bin/env python3.6
# -*- coding:utf-8 -*-
# __author__: sdc

import sys
import time
import json
import socket
import asyncio
import confluent_kafka
from asynckafka import Producer

# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()

async def error_callback(kafka_error):
    logger.error(kafka_error)

class UDPServer():
    def __init__(self, upload_speed=0, download_speed=0, recv_max_size=512 * 1024 ):
        self.upload_speed = upload_speed
        self.download_speed = download_speed
        self.recv_max_size = recv_max_size

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setblocking(False)

        self.send_event = asyncio.Event()
        self.send_queue = list()

        self.bytes_send = 0
        self.bytes_recv = 0

    def run(self, host, port, loop=None):
        self.loop = loop or asyncio.get_event_loop()

        self.sock.bind((host, port))

        self.connection_made()

        self._run_future(self._send_periodically())
        self._run_future(self._recv_periodically())

        if not loop:
            self.loop.run_forever()


    def send(self, data, addr):
        self.send_queue.append((data, addr))
        self.send_event.set()

    def _run_future(self, coro_or_future):
        asyncio.ensure_future(coro_or_future, loop=self.loop)

    def _sock_recv(self, fut=None, registered=False):
        fd = self.sock.fileno()

        if fut is None:
            fut = self.loop.create_future()

        if registered:
            self.loop.remove_reader(fd)

        try:
            data, addr = self.sock.recvfrom(self.recv_max_size)
        except (BlockingIOError, InterruptedError):
            self.loop.add_reader(fd, self._sock_recv, fut, True)
        except Exception as e:
            fut.set_result(0)
            self.socket_error(e)
        else:
            fut.set_result((data, addr))

        return fut

    def _sock_send(self, data, addr, fut=None, registered=False):
        fd = self.sock.fileno()

        if fut is None:
            fut = self.loop.create_future()

        if registered:
            self.loop.remove_writer(fd)

        if not data:
            return

        try:
            bytes_sent = self.sock.sendto(data, addr)
        except (BlockingIOError, InterruptedError):
            self.loop.add_writer(fd, self._sock_send, data, addr, fut, True)
        except Exception as e:
            fut.set_result(0)
            self.socket_error(e)
        else:
            fut.set_result(bytes_sent)

        return fut

    async def _throttle(self, data_len, speed=0):
        delay = (data_len / speed) if speed > 0 else 0
        await asyncio.sleep(delay)

    async def _send_periodically(self):
        while True:
            await self.send_event.wait()
            try:
                while self.send_queue:
                    data, addr = self.send_queue.pop()
                    bytes_sent = await self._sock_send(data, addr)
                    await self._throttle(bytes_sent, self.upload_speed)
            finally:
                self.send_event.clear()

    async def _recv_periodically(self):
        while True:
            data, addr = await self._sock_recv()
            self._run_future(self.datagram_received(data, addr))
            await self._throttle(len(data), self.download_speed)

    def connection_made(self):
        pass

    def socket_error(self, e):
        pass

    async def datagram_received(self, data, addr):
        try:
            host = addr[0]
            logger.info("\thost=%s\tmessage=%s" % (host, data))
        except:
            host = None
        try:
            data = data.decode()
        except:
            pass
        data = json.dumps({"data": data, "host": host})
        data = bytes(data, encoding="utf8")
        await in_queue(data)


async def in_queue(data):
    await producer.produce(origin_syslog, data)


if __name__ == '__main__':
    logger = config_env["logger"]
    web_ip = config_env["web_ip"]
    web_port = config_env["web_port"]
    kafka_ip = config_env["kafka_ip"]
    kafka_port = config_env["kafka_port"]
    origin_syslog = config_env["origin_syslog"]
    #producer = Producer(brokers="%s:%s" % (kafka_ip, kafka_port), error_callback=error_callback)
    producer = Producer(brokers="%s:%s" % (kafka_ip, kafka_port))
    producer.start()

    # Bandwidth speed is 100 bytes per second
    #server = UDPServer(download_speed=100, upload_speed=100)

    server = UDPServer()
    try:
        server.run(web_ip, web_port)
    finally:
        produer.stop()
        server.stop()

