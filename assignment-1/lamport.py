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
    def __init__(self, id, clock, halt, **kwargs):
        super().__init__(**kwargs)
        self.id = id
        self.clock = clock
        self.halt = halt
        self.sock = sk.socket(sk.AF_INET, sk.SOCK_DGRAM)
        self.sock.bind(("", PORTS[id]))
        self.acks = defaultdict(int)
        self.acknowledged = defaultdict(bool)
        self.queue = []

    def deliver(self, event):
        print(f"P{self.id}: Processed event P{event.pid}.{event.clock}")

    def acknowledge(self, event):
        with self.clock.get_lock():
            self.clock.value += 1
        msg = dict(type=MSG_TYPE_ACK, pid=event.pid, clock=event.clock)
        data = json.dumps(msg).encode()
        for port in PORTS:
            self.sock.sendto(data, ("localhost", port))
        self.acknowledged[event] = True

    def run(self):
        while not self.halt():
            # read the message
            data = self.sock.recvfrom(MAX_BUF_SIZE)[0]
            msg = json.loads(data.decode())

            # check if ack
            event = Event(clock=msg["clock"], pid=msg["pid"])
            if msg["type"] == MSG_TYPE_ACK:
                self.acks[event] += 1
                if self.queue:
                    if self.acks[self.queue[0]] >= NUM_MEMBERS:
                        self.deliver(heappop(self.queue))
                    elif not self.acknowledged[self.queue[0]]:
                        self.acknowledge(event)

            # msg is a new event
            elif msg["type"] == MSG_TYPE_EVENT:
                # update local clock
                with self.clock.get_lock():
                    self.clock.value += 1

                heappush(self.queue, event)
                self.acks[event] = 0

                if not self.acknowledged[event]:
                    self.acknowledge(event)


class MemberProcess(mp.Process):
    def __init__(self, id, **kwargs):
        super().__init__(**kwargs)
        self.id = id
        self.clock = mp.Value("i", 0)
        self.sock = sk.socket(sk.AF_INET, sk.SOCK_DGRAM)

    def do_operation(self):
        time.sleep(0.01)

    def broadcast_event(self):
        with self.clock.get_lock():
            self.clock.value += 1
        msg = dict(pid=self.id, clock=self.clock.value, type=MSG_TYPE_EVENT)
        data = json.dumps(msg).encode()
        for port in PORTS:
            self.sock.sendto(data, ("localhost", port))
        print(f"P{self.id}: Sent event P{self.id}.{msg['clock']}")

    def run(self):
        halt = False
        communication_thread = CommunicationThread(
            id=self.id, clock=self.clock, halt=lambda: halt
        )
        communication_thread.start()

        time.sleep(1)

        for _ in range(3):
            self.do_operation()
            self.broadcast_event()

        time.sleep(1)
        halt = True


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
