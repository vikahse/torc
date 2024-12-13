import time

import aiohttp
import asyncio
import random


class RaftClient:
    def __init__(self, peers):
        self.peers = peers  # Список всех нод
        self.master = random.choice(list(self.peers.keys()))  # Адрес мастера (изначально выбор случайно реплики)
        # self.peers[self.master] = True
        self.replica = random.choice(list(self.peers.keys()))

    async def read(self, key):
        """Чтение с реплики"""
        async with aiohttp.ClientSession() as session:
            peer = random.choice(list(self.peers.keys()))
            while peer == self.master:
                peer = random.choice(list(self.peers.keys()))
            print(peer)
            try:
                async with session.post(f"http://{peer}/client/read", json={"key": key}) as response:
                    data = await response.json()
                    if data.get("status") == "error" and data.get("message") == "I am master":
                        self.peers[self.master] = False
                        self.master = f"127.0.0.1:{data.get('master_id')}"
                        await self.read(key)
                    self.master = f"127.0.0.1:{data.get('master_id')}"
                    print(f"read response:", data)
                    return data
            except Exception as e:
                print(f"Ошибка чтения с реплики {peer}: {e}")
                await self.read(key)

    async def write(self, operation, key, value=None):
        """Запись на мастер"""
        async with aiohttp.ClientSession() as session:
            try:
                data = {"operation": operation, "key": key, "value": value}
                async with session.post(f"http://{self.master}/client/write", json=data) as response:
                    data = await response.json()
                    if data.get("status") == "error" and data.get("message") == "Not a master":
                        self.peers[self.master] = False
                        self.master = f"127.0.0.1:{data.get('master_id')}"
                        await self.write(operation, key, value)
                    print(f"{operation} response:", data)
                    return data
            except Exception as e:
                print(f"Ошибка записи на мастер {self.master}: {e}")
                self.master = random.choice(list(self.peers.keys()))
                await self.write(operation, key, value)

async def main():
    client = RaftClient(peers={"127.0.0.1:8002": False, "127.0.0.1:8003": False, "127.0.0.1:8001": False})

    # Запись
    response = await client.write("create", "key1", f"value1")
    # print("Create response:", response)

    print("*" * 10)
    time.sleep(50)

    response = await client.read(f"key1")
    # print("Read response:", response)

    # Обновление
    response = await client.write("update", f"key1", f"value1_new")
    # print("Update response:", response)

    print("*" * 10)
    time.sleep(50)

    response = await client.read(f"key1")
    # print("Read response:", response)

    # Удаление
    response = await client.write("delete", f"key1")
    # print("Delete response:", response)

    print("*" * 10)
    time.sleep(50)

    response = await client.read(f"key1")
    # print("Read response:", response)

    ######################

    # Запись
    response = await client.write("create", "key2", f"value2")
    # print("Create response:", response)

    print("*" * 10)
    time.sleep(50)

    response = await client.read(f"key2")
    # print("Read response:", response)

    # Обновление
    response = await client.write("update", f"key2", f"value2_new")
    # print("Update response:", response)

    print("*" * 10)
    time.sleep(50)

    response = await client.read(f"key2")
    # print("Read response:", response)

    # Удаление
    response = await client.write("delete", f"key2")
    # print("Delete response:", response)

    print("*" * 10)
    time.sleep(50)

    response = await client.read(f"key2")
    # print("Read response:", response)

if __name__ == "__main__":
    asyncio.run(main())
