import asyncio
import aiohttp
from aiohttp import web
import random
import time
import threading


class CRDTNode:
    def __init__(self, id, peers):
        self.id = id
        self.peers = peers
        self.operation_seq = 0
        self.send_seq = 0
        self.delivered = {}
        self.msg_buffer = []
        self.values = {}  # key -> [t, k, v]
        self.operations_log = []  # [{type:set, key:key1, value:value1, seg_number:2, from:self.id}]
        self.lock = threading.Lock()

    async def handle_client_write(self, request):
        """Обработка клиентских запросов на изменение записи по ключу"""
        data = await request.json()  # {key1:val1, key2:val2}
        with self.lock:
            for key, value in data.items():
                self.operation_seq += 1
                self.operations_log.append(
                    {"type": "set", "operation_seq": self.operation_seq, "key": key, "value": value, "from": self.id})
                await self.broadcast()

    async def handle_client_read(self, request):
        """Обработка клиентских запросов на чтение ключа по значению"""
        data = await request.json()
        with self.lock:
            key = data.get("key")
            if key not in self.values:
                return web.json_response(
                    {"status": "error", "message": f"Key {key} not found."}, status=404)
            return web.json_response(
                {"status": "success", "key": key, "value": self.values[key][2]}, status=200)

    async def handle_delivering(self, request):
        data = await request.json()  # [{}, {},...]
        with self.lock:
            self.msg_buffer.append(data)
            for operation in data:
                key = operation.get("key")
                value = operation.get("value")
                operation_seq = operation.get("operation_seq")
                type = operation.get("type")
                replica_from = operation.get("from")
                if key not in self.values:
                    self.values[key] = [operation_seq, key, value]
                    # распространяем между другими ?
                    self.operations_log.append(
                        {"type": type, "operation_seq": operation_seq, "key": key, "value": value,
                         "from": replica_from})
                    return web.json_response(
                        {"status": "success", "message": "changed"}, status=200)
                else:
                    cur_operation_seq, cur_key, cur_value = self.values[key][0], self.values[key][1], self.values[key][
                        2]
                    if cur_operation_seq < operation_seq:  # lww
                        self.values[key] = [operation_seq, key, value]
                        # распространяем между другими ?
                        self.operations_log.append(
                            {"type": type, "operation_seq": operation_seq, "key": key, "value": value,
                             "from": replica_from})
                        return web.json_response(
                            {"status": "success", "message": "changed"}, status=200)
                return web.json_response(
                    {"status": "error", "message": "old number"}, status=400)

    async def broadcast(self):  # reliable casual order broadcast
        with self.lock:
            all_nodes = peers
            all_nodes.append(f"127.0.0.1:{self.id}")
            to_send = self.operations_log
            for peer in all_nodes:
                deps = self.delivered
                deps[self.id] = self.send_seq
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(f"http://{peer}/delivering",
                                                json={"sender": self.id, "deps": deps, "msg": to_send}) as response:
                            data = await response.json()
                            self.send_seq += 1
                            print(f"Node {self.id}: Отправил broadcast {peer} и получил ответ {data}")
                except Exception as e:
                    print(f"Node {self.id}: Не удалось отправить broadcast {peer}: {e}")

    async def run(self):
        while True:
            await self.broadcast()
            await asyncio.sleep(5)

    def start_server(self):
        """Запуск HTTP-сервера"""
        app = web.Application()
        app.router.add_get("/client/read", self.handle_client_read)
        app.router.add_patch("/client/write", self.handle_client_write)
        app.router.add_post("/delivering", self.handle_delivering)
        return app


async def main(port, peers):
    node = CRDTNode(port, peers)
    app = node.start_server()

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    print(f"Node {port} started with peers {peers}")
    await node.run()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True, help="Port to run the node")
    parser.add_argument("--peers", type=str, required=True, help="Comma-separated list of peer addresses")
    args = parser.parse_args()

    port = args.port
    peers = args.peers.split(",")

    asyncio.run(main(port, peers))
