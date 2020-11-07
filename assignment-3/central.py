#!/usr/bin/env python3

import multiprocessing as mp
import socket
from pathlib import Path

# global constants
COORDINATOR_IP = "127.0.0.1"
COORDINATOR_PORT = 11111
NUM_MEMBERS = 5
MAX_BUFFER_SIZE = 1024


class CoordinatorProcess(mp.Process):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.request_queue = []
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((COORDINATOR_IP, COORDINATOR_PORT))
        self.sock.listen(NUM_MEMBERS)

    def run(self):
        while True:
            conn = self.sock.accept()[0]
            msg = conn.recv(MAX_BUFFER_SIZE).decode().strip().lower()
            if msg == "request":
                if not self.request_queue:  # queue is empty
                    conn.send("ok".encode())
                self.request_queue.append(conn)
            elif msg == "release":
                self.request_queue.pop(0)
                if self.request_queue:  # queue is not empty
                    conn = self.request_queue[0]
                    conn.send("ok".encode())
            else:
                print("Coordinator: ERROR: invalid message from client")


class MemberProcess(mp.Process):
    def __init__(self, id, **kwargs):
        super().__init__(**kwargs)
        self.id = id

    def acquire_lock(self):
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect((COORDINATOR_IP, COORDINATOR_PORT))
        conn.send("request".encode())
        reply = conn.recv(MAX_BUFFER_SIZE).decode().strip().lower()
        conn.close()
        return reply == "ok"

    def release_lock(self):
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect((COORDINATOR_IP, COORDINATOR_PORT))
        conn.send("release".encode())
        conn.close()

    def run(self):
        print(f"Member {self.id} started")
        for _ in range(10):
            if self.acquire_lock():
                print(f"Lock acquired by Member {self.id}")
                number = int(Path("data").read_text())
                number += 1
                Path("data").write_text(f"{number}\n")
                self.release_lock()
            else:
                print(f"Member {self.id}: ERROR: failed to acquire lock")
        print(f"Member {self.id} done")


if __name__ == "__main__":
    # create a data file
    Path("data").write_text("0\n")

    # create the coordinator and members
    coord = CoordinatorProcess()
    members = [MemberProcess(id=i) for i in range(NUM_MEMBERS)]

    # start the processes
    coord.start()
    for member in members:
        member.start()

    # wait for members to complete
    for member in members:
        member.join()
    coord.terminate()
