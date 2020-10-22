#!/usr/bin/env python3

import multiprocessing as mp
import select
import socket
import time
from pathlib import Path
from queue import PriorityQueue
from threading import Thread

# global constants
NUM_MEMBERS = 5
MAX_BUFFER_SIZE = 1024
RECV_PORTS = [10000 + i * 2 for i in range(NUM_MEMBERS)]


class MemberProcess(mp.Process):
    def __init__(self, id, **kwargs):
        super().__init__(**kwargs)
        self.id = id
        self.request_timestamp = 0
        self.pending_queue = None
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", RECV_PORTS[self.id]))
        # self.sock.settimeout(1)

    def server_function(self, halt):
        while not halt():
            ready = select.select([self.sock], [], [], 1)
            if not ready[0]:
                continue
            data, client_addr = self.sock.recvfrom(MAX_BUFFER_SIZE)
            tokens = data.decode().strip().lower().split()
            if tokens[0] == "request":
                if self.request_timestamp == 0:  # we don't want lock
                    self.sock.sendto("ok".encode(), client_addr)
                elif int(tokens[1]) < self.request_timestamp:
                    self.sock.sendto("ok".encode(), client_addr)
                else:
                    # add the request to queue
                    timestamp = int(tokens[1])
                    self.pending_queue.put((timestamp, client_addr))

    def acquire_lock(self):
        self.request_timestamp = time.time_ns()

        # create client socks
        socks = [
            socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            for id in range(NUM_MEMBERS)
        ]

        # send requests
        for id, sock in enumerate(socks):
            if id != self.id:
                sock.sendto(
                    f"request {self.request_timestamp}".encode(),
                    ("127.0.0.1", RECV_PORTS[id]),
                )
                # print(f"Lock requested by Member {self.id} from {id}")

        # get acknowledgments
        def get_ack(sock):
            sock.recvfrom(MAX_BUFFER_SIZE)
            sock.close()

        ack_threads = [
            Thread(target=get_ack, args=(sock,))
            for id, sock in enumerate(socks)
            if id != self.id
        ]
        for ack_thread in ack_threads:
            ack_thread.start()
        for ack_thread in ack_threads:
            ack_thread.join()

        print(f"Lock acquired by Member {self.id}")
        return True

    def release_lock(self):
        # print(f"Lock released by Member {self.id}")
        while not self.pending_queue.empty():
            client_addr = self.pending_queue.get()[1]
            self.sock.sendto("ok".encode(), client_addr)
        self.request_timestamp = 0

    def run(self):
        self.pending_queue = PriorityQueue(NUM_MEMBERS)
        halt = False
        server_thread = Thread(target=self.server_function, args=(lambda: halt,))
        server_thread.start()
        print(f"Starting Member {self.id}...")

        time.sleep(1)

        for _ in range(10):
            if self.acquire_lock():
                number = int(Path("data").read_text())
                number += 1
                Path("data").write_text(f"{number}\n")
                self.release_lock()
            else:
                print(f"Member {self.id}: ERROR: failed to acquire lock")
        print(f"Member {self.id} done")

        time.sleep(1)
        halt = True
        server_thread.join()


if __name__ == "__main__":
    # create a data file
    Path("data").write_text("0\n")

    # create the coordinator and members
    members = [MemberProcess(id=i) for i in range(NUM_MEMBERS)]

    # start the processes
    for member in members:
        member.start()
        # time.sleep(0.01)

    # wait for members to complete
    for member in members:
        member.join()
