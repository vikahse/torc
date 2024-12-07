import asyncio
import aiohttp
from aiohttp import web
import random
import time
import threading

class RaftNode:
    def __init__(self, port, peers):
        self.port = port
        self.peers = peers
        self.current_term = 0
        self.voted_for = None
        self.state = "follower"  # "follower", "leader", "candidate"
        self.leader_id = None
        self.log = []  # Лог для репликации
        self.data_store = {}  # Локальное хранилище данных
        self.commit_index = -1
        self.last_applied = -1
        self.next_index = {}
        self.match_index = {}
        self.last_heartbeat = time.time()
        self.timeout = random.uniform(5.0, 7.0)
        self.is_master = False  # Флаг для мастера

    async def handle_heartbeat(self, request):
        """Обработка сердцебиений от лидера"""
        data = await request.json()
        if data['term'] >= self.current_term:
            self.last_heartbeat = time.time()
            self.state = "follower"
            if self.leader_id != data['leader_id']:
                self.leader_id = data['leader_id']
                self.is_master = (self.port == self.leader_id)
                print(f"Node {self.port}: Новый лидер: {self.leader_id}")
            self.current_term = data['term']
            prev_log_index = data['prev_log_index']
            prev_log_term = data['prev_log_term']
            entries = data['entries']
            leader_commit = data['leader_commit']
            if prev_log_index >= 0:
                if len(self.log) <= prev_log_index or self.log[prev_log_index].get('term') != prev_log_term:
                    return web.json_response({"success": False})
            for entry in entries:
                # if len(self.log) > prev_log_index + 1:
                #     if self.log[prev_log_index + 1]['term'] != entry['term']:
                #         self.log = self.log[:prev_log_index + 1]
                #         self.log.append(entry)
                # else:
                self.log.append(entry)
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)
                self.apply_log()
            return web.json_response({"success": True})
        else:
            print(f"Node {self.port}: Игнорирую сердцебиение от устаревшего лидера {data['leader_id']}")
            return web.json_response({"success": False})

    async def handle_vote_request(self, request):
        """Обработка запросов на голосование"""
        data = await request.json()
        candidate_id = data['candidate_id']
        term = data['term']

        if term > self.current_term or (term == self.current_term and self.voted_for is None):
            self.current_term = term
            self.voted_for = candidate_id
            print(f"Node {self.port}: Проголосовал за {candidate_id} в термине {term}")
            return web.json_response({"vote_granted": True})

        return web.json_response({"vote_granted": False})

    async def handle_client_read(self, request):
        """Обработка клиентских запросов на чтение (реплики)"""
        if self.is_master:
            return web.json_response({"status": "error", "message": f"I am master", "master_id": self.port}, status=404)
        data = await request.json()
        key = data.get("key")
        if key not in self.data_store:
            return web.json_response({"status": "error", "message": f"Key {key} not found.", "master_id": self.leader_id}, status=404)
        return web.json_response({"status": "success", "key": key, "value": self.data_store[key], "master_id": self.leader_id})

    async def handle_client_write(self, request):
        """Обработка клиентских запросов на запись (мастер)"""
        if not self.is_master:
            return web.json_response({"status": "error", "message": "Not a master", "master_id": self.leader_id}, status=403)

        data = await request.json()
        operation = data.get("operation")
        key = data.get("key")
        value = data.get("value")

        # Добавление операции в лог
        if operation == "create":
            # if key in self.data_store:
            #     return web.json_response({"status": "error", "message": f"Key {key} already exists."}, status=409)
            self.log.append({"operation": "create", "key": key, "value": value, "term": self.current_term})
        elif operation == "update":
            # if key not in self.data_store:
            #     return web.json_response({"status": "error", "message": f"Key {key} not found."}, status=404)
            self.log.append({"operation": "update", "key": key, "value": value, "term": self.current_term})
        elif operation == "delete":
            # if key not in self.data_store:
            #     return web.json_response({"status": "error", "message": f"Key {key} not found."}, status=404)
            self.log.append({"operation": "delete", "key": key, "term": self.current_term})
        else:
            return web.json_response({"status": "error", "message": "Invalid operation"}, status=400)

        # Репликация изменений
        threading.Thread(target=self.replicate_log, daemon=True).start()
        # self.replicate_log()
        return web.json_response({"status": "success", "message": "Master add command to the log", "master_id": self.leader_id}, status=400)

    def apply_log(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            # Применяем команду к состоянию
            operation = entry.get("operation")
            key = entry.get("key")
            if operation == "create":
                if key not in self.data_store:
                    value = entry.get("value")
                    self.data_store[key] = value
            elif operation == "update":
                if key in self.data_store:
                    value = entry.get("value")
                    self.data_store[key] = value
            elif operation == "delete":
                if key in self.data_store:
                    del self.data_store[key]
            print(f"Узел {self.port} применил команду: {operation}")

    def replicate_log(self):
        """Репликация лога на реплики"""
        print(f"Узел {self.port} начинает репликацию логов")
        while True:
            success_count = 1  # Считаем себя
            for peer in self.peers:
                if self.match_index.get(peer, -1) >= len(self.log) - 1:
                    success_count += 1
            if success_count > len(self.peers) // 2:
                self.commit_index = len(self.log) - 1
                print(f"Узел {self.port} будет применять лог")
                self.apply_log()
                break
            time.sleep(0.1)

    async def send_heartbeat(self):
        """Отправка сердцебиений от лидера"""
        if self.state != "leader":
            return

        for peer in self.peers:
            prev_log_index = self.next_index.get(peer, 0) - 1
            prev_log_term = self.log[prev_log_index].get("term") if prev_log_index >= 0 and prev_log_index < len(self.log) else -1
            entries = self.log[prev_log_index + 1:]
            print(f"Node {self.port}: peer {peer}, prev_log_index {prev_log_index}, entries {entries}")
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(f"http://{peer}/heartbeat", json={
                        "term": self.current_term,
                        "leader_id": self.port,
                        "prev_log_index": prev_log_index,
                        "prev_log_term": prev_log_term,
                        "entries": entries,
                        "leader_commit": self.commit_index
                    }) as response:
                        data = await response.json()
                        if data.get("success"):
                            self.match_index[peer] = self.next_index.get(peer, 0) + len(entries) - 1
                            self.next_index[peer] = self.match_index[peer] + 1
                        else:
                            self.next_index[peer] = max(0, self.next_index.get(peer, 0) - 1)
                    print(f"Node {self.port}: Успешное сердцебиение с {peer}")
            except Exception as e:
                print(f"Node {self.port}: Не удалось отправить сердцебиение узлу {peer}: {e}")

    async def request_votes(self):
        """Запрос голосов для выбора нового мастера"""
        self.current_term += 1
        self.voted_for = self.port
        votes = 1

        session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=5, sock_read=5)

        for peer in self.peers:
            try:
                async with aiohttp.ClientSession(timeout=session_timeout) as session:
                    async with session.post(f"http://{peer}/vote", json={
                        "term": self.current_term,
                        "candidate_id": self.port,
                    }) as response:
                        data = await response.json()
                        if data.get("vote_granted"):
                            votes += 1
            except Exception as e:
                print(f"Node {self.port}: Не удалось запросить голос у {peer}: {e}")

        if votes > len(self.peers) // 2:
            print(f"Node {self.port}: Я лидер в термине {self.current_term}")
            self.state = "leader"
            self.leader_id = self.port
            self.is_master = True
            for peer in self.peers:
                self.next_index[peer] = len(self.log)  # last log index + 1
                self.match_index[peer] = -1
        else:
            print(f"Node {self.port}: Не смог стать лидером")
            self.state = "follower"
            self.is_master = False

    async def run_follower(self):
        """Работа узла в режиме follower"""
        while self.state == "follower":
            if time.time() - self.last_heartbeat > self.timeout:
                print(f"Node {self.port}: Таймаут, начинаю выборы")
                self.state = "candidate"
                await asyncio.sleep(random.uniform(1.0, 2.0))  # Задержка перед выборами
                await self.request_votes()
            await asyncio.sleep(0.5)

    async def run_candidate(self):
        """Работа узла в режиме candidate"""
        await self.request_votes()

    async def run_leader(self):
        """Работа узла в режиме leader"""
        while self.state == "leader":
            print(f"Node {self.port}: Я лидер")
            await self.send_heartbeat()
            await asyncio.sleep(1)

    async def run(self):
        """Основной цикл работы узла"""
        while True:
            if self.state == "follower":
                await self.run_follower()
            elif self.state == "candidate":
                await self.run_candidate()
            elif self.state == "leader":
                await self.run_leader()

    def start_server(self):
        """Запуск HTTP-сервера"""
        app = web.Application()
        app.router.add_post("/heartbeat", self.handle_heartbeat)
        app.router.add_post("/vote", self.handle_vote_request)
        app.router.add_post("/client/read", self.handle_client_read)
        app.router.add_post("/client/write", self.handle_client_write)
        return app


async def main(port, peers):
    node = RaftNode(port, peers)
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
