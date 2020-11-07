import json
import multiprocessing as mp
import socket as sk
import threading as tr
import time
from collections import defaultdict, namedtuple
from heapq import heappop, heappush

NUM_MEMBERS = 3
PORTS = [10000 + i * 2 for i in range(NUM_MEMBERS)]
MAX_BUF_SIZE = 1024

MSG_TYPE_EVENT = 0
MSG_TYPE_ACK = 1

Event = namedtuple("Event", ["clock", "pid"])


class CommunicationThread(tr.Thread):
    def __init__(self, id, clock, **kwargs):
        super().__init__(**kwargs)
        self.id = id
        self.clock = clock
        self.sock = sk.socket(sk.AF_INET, sk.SOCK_DGRAM)
        self.sock.bind(("", PORTS[id]))
        self.acks = defaultdict(int)
        self.acknowledged = defaultdict(bool)
        self.queue = []

    def deliver(self, event):
        pid = event["pid"]
        print("P{}: Processed event P{}.{}".format(self.id, pid, event["clock"][pid]))

    def run(self):
        while True:
            # read the message
            data = self.sock.recvfrom(MAX_BUF_SIZE)[0]
            msg = json.loads(data.decode())
            self.queue.append(msg)

            ready_indices = []
            for i, m in enumerate(self.queue):
                pid = m["pid"]
                if pid != self.id and m["clock"][pid] != self.clock[pid] + 1:
                    continue
                for k in range(NUM_MEMBERS):
                    if k != pid and m["clock"][pid] > self.clock[pid]:
                        continue
                ready_indices.append(i)
                self.deliver(m)

            for i in ready_indices[::-1]:
                self.queue.pop(i)

            # update local vector clock
            for id in range(NUM_MEMBERS):
                self.clock[id] = max(self.clock[id], msg["clock"][id])


class MemberProcess(mp.Process):
    def __init__(self, id, **kwargs):
        super().__init__(**kwargs)
        self.id = id
        self.clock = [0] * NUM_MEMBERS
        self.sock = sk.socket(sk.AF_INET, sk.SOCK_DGRAM)

    def do_operation(self):
        time.sleep(0.01)

    def broadcast_event(self):
        self.clock[self.id] += 1
        msg = dict(pid=self.id, clock=self.clock)
        print(f"P{self.id}: Creating event P{self.id}.{msg['clock'][self.id]}")
        data = json.dumps(msg).encode()
        for port in PORTS:
            self.sock.sendto(data, ("localhost", port))

    def run(self):
        communication_thread = CommunicationThread(id=self.id, clock=self.clock)
        communication_thread.start()

        time.sleep(1)

        for _ in range(3):
            self.do_operation()
            self.broadcast_event()


if __name__ == "__main__":
    # create members
    members = [MemberProcess(id=i) for i in range(NUM_MEMBERS)]

    # start the processes
    for member in members:
        member.start()
        time.sleep(0.01)

    # wait for members to complete
    for member in members:
        member.join()
