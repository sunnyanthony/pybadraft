import asyncio
import datetime
from typing import List, Optional, Tuple
from functools import partial
from loguru import logger
import socket
import threading
import time
import random
from .decorator import synchronizer
from .roles import CandidateState, FollowerState, LeaderState, NodeState
from .packet import vote_request, vote, load_packet, heartbeat, Request, MetaData

class RaftNode:
    def __init__(self, id: int, port: int, peers: List[Tuple[str, int]], init_timeout: float=0) -> None:
        self.id: int = id
        self.port: int = port
        self.peers: List[Tuple[str, int]] = peers
        self.peers_status: List[int] = [0] * len(peers)
        self._state: NodeState = FollowerState()
        self.term: int = 0
        self.leader: int = -1
        self._voted_for: Optional[int] = None
        self._voted_for_timeout = datetime.datetime.now().timestamp()
        self.votes_received: int = 0
        self.server_timeout: int = 3
        self.election_timeout: float = random.randint(150, 300) / 1000.0
        self.heartbeat_interval = 50/1000
        self.voting_timeout = random.randint(100, int(self.election_timeout*1000)) / 1000.0
        self.server = None
        self.server_loop = None
        self.lock: threading.RLock = threading.RLock()
        self.threads: List[threading.Thread] = []
        self.stop_signal: threading.Event = threading.Event()
        self.heartbeat_timer: Optional[threading.Thread] = None
        self.election_skip = datetime.datetime.now().timestamp()
        self.election_timer: Optional[threading.Thread] = None
        self._state.on_enter_state(self)
        logger.info(f"RaftNode {self.id} initialized with state: {self.state.__name__}, term: {self.term}")

    @property
    def voted_for(self) -> type:
        res = None
        if self._voted_for_timeout >= datetime.datetime.now().timestamp():
            res = self._voted_for
        return res

    @voted_for.setter
    def voted_for(self, value) -> None:
        self._voted_for_timeout = datetime.datetime.now().timestamp() + self.voting_timeout
        self._voted_for = value

    @property
    def state(self) -> type:
        return self._state.__class__

    @state.setter
    @synchronizer('lock')
    def state(self, value: type[NodeState]) -> None:
        if self._state.__class__ != value:
            self._state.on_exit_state(self)
            self._state = value()
            self._state.on_enter_state(self)
            logger.info(f"Node {self.id} state changed to {self._state.__class__.__name__}")

    def check_votes(self) -> None:
        if self.state == CandidateState and self.votes_received > len(self.peers) / 2:
            self.state = LeaderState
            logger.info(f"{self.id} is now the leader for term {self.term}.")

    def request_votes(self) -> None:
        threads = []
        for peer in self.peers:
            threads.append(threading.Thread(target=partial(self.send_vote_request, peer=peer)))
            threads[-1].start()
        
        for thread in threads:
            thread.join()

    def send_vote_request(self, peer: Tuple[str, int]) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.settimeout(self.election_timeout)
                s.connect(('localhost', peer[1]))
                s.sendall(vote_request(self.term, self.id))
                logger.debug(f"{self.id} sent vote request to {peer}")
                data = s.recv(1024)
                response_data = load_packet(data)
                if response_data.type == Request.VOTE_GRANTED and response_data.term == self.term and response_data.granted:
                    self.increment_votes()
            except ConnectionRefusedError:
                logger.error(f"{self.id} could not connect to {peer}")
            except socket.timeout:
                logger.error(f"Connection to {peer[0]} timed out")
            except Exception as e:
                logger.error(f"An error occurred while connecting or communicating with {peer[0]}: {str(e)}")

    def increment_votes(self) -> None:
        self.votes_received += 1
        if self.votes_received > len(self.peers) / 2:
            logger.info(f"{self.id} has received majority votes")
        self.check_votes()

    async def handle_vote_request(self, data: MetaData,  writer: asyncio.StreamWriter) -> None:
        term = data.term
        candidate_id = data.id
        vote_granted = False
        with self.lock:
            if (self.term < term) and (self.voted_for is None or self.voted_for == candidate_id):
                self.term = term
                self.voted_for = candidate_id
                self.state = FollowerState
                vote_granted = True
        if vote_granted:
            await asyncio.sleep(0)
            self.reset_election_timer()
            logger.info(f"Vote granted by {self.id} to {candidate_id} for term {term}")
        else:
            logger.info(f"Vote denied by {self.id} to {candidate_id} for term {term}")
            logger.debug(f"Because of {self.term}, {self.voted_for}")
        writer.write(vote(vote_granted, self.term, self.id))
        await writer.drain()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        data_len = len(MetaData().pack_data())
        data = bytearray()
        try:
            while not self.stop_signal.is_set():
                try:
                    data = await asyncio.wait_for(reader.readexactly(data_len), timeout=1.0)
                    metadata = load_packet(data)
                    if metadata.type == Request.VOTE_REQUEST:
                        await self.handle_vote_request(metadata, writer)
                    elif metadata.type == Request.HEARTBEAT:
                        self.reset_state_on_heartbeat(metadata)
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            logger.error(f"End... handler")
        except Exception as e:
            if not data:
                # check readexactly exception
                logger.debug(f"{e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                logger.error(f"{e}")

    def reset_state_on_heartbeat(self, data: MetaData) -> None:
        with self.lock:
            if data.term >= self.term:
                self.term = data.term
                if self.state == CandidateState:
                    self.state = FollowerState
                    logger.info(f"Received heartbeat, reset state to FOLLOWER for {self.id}")
        if data.term >= self.term:
            self._state.handle_request(self, data)
            self.election_skip = datetime.datetime.now().timestamp()

    def start_server(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.server_loop = loop

        async def server_coro():
            try:
                self.server = await asyncio.start_server(self.handle_client, 'localhost', self.port)
                async with self.server:
                    await self.server.serve_forever()
            except asyncio.CancelledError:
                logger.debug(f"Node {self.id} close server")
                raise asyncio.CancelledError
            except Exception as e:
                logger.error(f"{e}")
            finally:
                self.server.close()
                await self.server.wait_closed()

        try:
            loop.run_until_complete(server_coro())
        except asyncio.CancelledError:
            pass
        finally:
            loop.close()
            logger.debug(f"Event loop closed {self.server.is_serving()}")

    async def _heartbeat_async(self):
        tasks = []
        for idx, peer in enumerate(self.peers):
            task = asyncio.create_task(self._send_heartbeat_to_peer(peer, idx))
            tasks.append(task)
        await asyncio.gather(*tasks)

    async def _send_heartbeat_to_peer(self, peer, idx):
        writer = None
        try:
            _, writer = await asyncio.open_connection(peer[0], peer[1])
            writer.write(heartbeat(self.id, self.term))
            await writer.drain()
            self.peers_status[idx] = 0
        except OSError as e:
            if self.peers_status[idx] == 0:
                logger.error(f"Sending heartbeat to {peer[0]} failed: {str(e)}")
                self.peers_status[idx] = 1
        finally:
            if writer:
                writer.close()
                await writer.wait_closed()

    def send_heartbeat(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def heartbeat_loop():
            while self.state == LeaderState and not self.stop_signal.is_set():
                await self._heartbeat_async()
                # in short time calling to_thread will overwhelme the coroutine
                #await asyncio.to_thread(self._state.handle_request, self)
                await asyncio.sleep(self.heartbeat_interval)

        loop.run_until_complete(heartbeat_loop())

    def start_heartbeat(self) -> None:
        #while self.heartbeat_timer.is_alive():
        # TODO: avoid multiple heartbeat tasks   
        self.heartbeat_timer = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_timer.start()

    def start_election(self) -> None:
        should_request_votes = False
        if self.stop_signal.is_set():
            return

        with self.lock:
            diff = datetime.datetime.now().timestamp() - self.election_skip
            if self.state != LeaderState and (self.voted_for is None or self.voted_for == self.id) and diff > self.election_timeout:
                self.state = CandidateState
                self.term += 1
                should_request_votes = True
                self.voted_for = self.id
                self.votes_received = 1
                logger.info(f"{self.id} becomes a candidate and starts election for term {self.term}.")

        if should_request_votes:
            self.request_votes()

    def election_async_task(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def election_loop() -> None:
            while self.state != LeaderState and not self.stop_signal.is_set():
                await asyncio.sleep(self.election_timeout + random.randint(10, 100) / 1000.0)
                now = datetime.datetime.now().timestamp()
                if self.election_skip and self.election_skip >= now:
                    continue
                self.start_election()

        loop.run_until_complete(election_loop())

    def reset_election_timer(self) -> None:
        time.sleep(0)
        self.election_skip = datetime.datetime.now().timestamp() + self.election_timeout
        if not self.election_timer:
            self.election_timer = threading.Thread(target=self.election_async_task)
            self.election_timer.start()

    def _run(self) -> None:
        self.stop_signal.clear()
        thread = threading.Thread(target=self.start_server)
        thread.start()
        if self.state != LeaderState:
            self.reset_election_timer()

        self.threads.append(thread)
        while not self.stop_signal.is_set():
            time.sleep(1)

    def run(self, blocking: bool=False) -> None:
        logger.info(f"Node {self.id} starting...")
        thread = threading.Thread(target=self._run, daemon=True)
        self.threads.append(thread)
        thread.start()
        if blocking:
            for t in self.threads:
                t.join()

    async def stop_server(self):
        tasks = [t for t in asyncio.all_tasks(self.server_loop) if t is not asyncio.current_task()]

        for task in tasks:
            await asyncio.sleep(0)
            if not task.done():
                logger.debug(task.cancel())
        
        # FIXME: can't wait the server back but server closed
        #await asyncio.gather(*tasks, return_exceptions=True)

    def stop(self) -> None:
        logger.info(f"Node {self.id} stopping")
        self.election_skip = datetime.datetime.now().timestamp()
        self.stop_signal.set()
        logger.debug(f"{self.id} closing heartbeat")
        if self.heartbeat_timer:
            self.heartbeat_timer.join()
        if self.server_loop:
            asyncio.run_coroutine_threadsafe(self.stop_server(), self.server_loop).result()
        logger.debug(f"{self.id} waiting threads")
        for thread in self.threads:
            thread.join()
        self.state = FollowerState
        self.threads.clear()
        logger.info(f"Node {self.id} stopped")
