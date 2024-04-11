from loguru import logger
import asyncio
import threading
import random
from typing import List, Optional, Union
from .decorator import synchronizer
from .roles import CandidateState, FollowerState, LeaderState, NodeState
from .packet import vote_request, vote, load_packet, heartbeat, Request, MetaData

class RaftNode:
    def __init__(self, id: int, port: int, peers: List[tuple], init_timeout: float = 0) -> None:
        self.id: int = id
        self.port: int = port
        self.peers: List[tuple] = peers
        self.peers_status: List[int] = [0] * len(peers)
        self._state: NodeState = FollowerState()
        self.term: int = 0
        self.leader: int = -1
        self.voted_for: Optional[int] = None
        self.votes_received: int = 0
        self.server_timeout: int = 3
        self.election_timeout: float = random.randint(150, 300) / 1000.0
        self.lock: asyncio.Lock = asyncio.Lock()
        self.threads: List[threading.Thread] = []
        self.stop_signal: asyncio.Event = asyncio.Event()
        self.heartbeat_timer: Optional[asyncio.Task] = None
        self.election_timer: Optional[asyncio.Task] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.should_start_election: bool = True
        self.server = None
        logger.info(f"RaftNode {self.id} initialized with state: {self.state.__name__}, term: {self.term}")

    @property
    def state(self) -> type:
        return self._state.__class__

    async def set_state(self, value: type[NodeState]) -> None:
        locked: bool = False
        if not self.lock.locked():
            locked = True
            await self.lock.acquire()
        try:
            if self._state.__class__ != value:
                self._state.on_exit_state(self)
                self._state: NodeState = value()
                self._state.on_enter_state(self)
                logger.info(f"Node {self.id} state changed to {self._state.__class__.__name__}")
        finally:
            if locked:
                await self.lock.release()

    async def election_timer_method(self) -> None:
        try:
            while self.state != LeaderState and not self.stop_signal.is_set():
                if self.state == CandidateState:
                    timeout: float = self.election_timeout + random.randint(10, 100) / 1000.0
                else:
                    timeout: float = self.election_timeout
                await asyncio.sleep(timeout)
                if not self.should_start_election:
                    self.should_start_election = True
                    continue
                logger.debug(f"{self.id} starting ele")
                await self.start_election()
                await asyncio.sleep(timeout / 3)
                self.give_up_election()
        except asyncio.CancelledError:
            logger.debug(f"Election timer for Node {self.id} was cancelled.")
        except Exception as e:
            logger.error(f"{e}")

    async def heartbeat_timer_method(self) -> None:
        try:
            while self.state == LeaderState and not self.stop_signal.is_set():
                await asyncio.sleep(50 / 1000)
                await self.state_check()
        except asyncio.CancelledError:
            logger.debug(f"Heartbeat timer for Node {self.id} was cancelled.")

    async def check_votes(self) -> None:
        if self.state == CandidateState and self.votes_received > len(self.peers) / 2:
            await self.set_state(LeaderState)
            logger.info(f"{self.id} is now the leader for term {self.term}.")

    async def send_vote_request(self, peer: tuple) -> None:
        writer: Optional[asyncio.StreamWriter] = None
        try:
            logger.debug(f"{self.id} sending vote request")
            reader, writer = await asyncio.open_connection('localhost', peer[1])
            writer.write(vote_request(self.term, self.id))
            await writer.drain()
            data = await reader.read(1024)
            response_data = load_packet(data)
            if response_data.type == Request.VOTE_GRANTED and response_data.term == self.term and response_data.granted:
                await self.increment_votes()
        except (ConnectionRefusedError, asyncio.TimeoutError) as e:
            logger.error(f"{self.id} could not connect to {peer} or connection timed out: {str(e)}")
        except Exception as e:
            logger.error(f"An error occurred while connecting or communicating with {peer}: {str(e)}")
        finally:
            if writer:
                writer.close()
                await writer.wait_closed()

    async def request_votes(self) -> None:
        tasks: List[asyncio.Task] = []
        logger.debug(f"{self.id} create vote task")
        for peer in self.peers:
            task = asyncio.create_task(self.send_vote_request(peer))
            tasks.append(task)
        await asyncio.gather(*tasks)

    @synchronizer('lock')
    async def increment_votes(self) -> None:
        self.votes_received += 1
        await self.check_votes()

    async def handle_vote_request(self, data: MetaData, writer: asyncio.StreamWriter) -> None:
        async with self.lock:
            term: int = data.term
            candidate_id: int = data.id
            vote_granted: bool = False
            if (self.term < term) and (self.voted_for is None or self.voted_for == candidate_id):
                self.term = term
                self.voted_for = candidate_id
                await self.set_state(FollowerState)
                vote_granted = True
                self.reset_election_timer()
                logger.info(f"Vote granted by {self.id} to {candidate_id} for term {term}")
            else:
                logger.info(f"Vote denied by {self.id} to {candidate_id} for term {term}")
                logger.debug(f"Because of {self.term}, {self.voted_for}")
            response_data = vote(vote_granted, self.term, self.id)
            
        writer.write(response_data)
        await writer.drain()

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        addr = writer.get_extra_info('peername')
        #logger.info(f"Received connection from {addr}")
        try:
            while not self.stop_signal.is_set():
                data = await reader.read(1024)
                if not data:
                    break
                metadata = load_packet(data)
                if metadata.type == Request.VOTE_REQUEST:
                    await self.handle_vote_request(metadata, writer)
                elif metadata.type == Request.HEARTBEAT:
                    await self.reset_state_on_heartbeat(metadata)
        except Exception as e:
            logger.error(f"Error handling connection: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            #logger.info(f"Connection closed for {addr}")

    async def reset_state_on_heartbeat(self, data: MetaData) -> None:
        if data.term >= self.term:
            self.should_start_election = False
            self.term = data.term
            if self.state == CandidateState:
                await self.state(FollowerState)
                logger.info(f"Received heartbeat, reset state to FOLLOWER for {self.id}")
            self._state.handle_request(self, data)

    async def start_server(self) -> None:
        server = await asyncio.start_server(
            self.handle_connection, 'localhost', self.port)
        logger.info(f"Server started on {self.port}")
        self.server = server

        async with server:
            await server.serve_forever()

    async def send_heartbeat_to_peer(self, idx: int, peer: tuple) -> None:
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection('localhost', peer[1]),
                timeout=0.01
            )
            writer.write(heartbeat(self.id, self.term))
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            self.peers_status[idx] = 0
        except asyncio.TimeoutError:
            logger.error(f"Node {self.id} connection to {peer[1]} timed out")
            self.peers_status[idx] = 1
        except ConnectionRefusedError:
            if self.peers_status[idx] == 0:
                logger.error(f"Node {self.id} could not connect to {peer[1]}")
                self.peers_status[idx] = 1
        except Exception as e:
            logger.error(f"Node {self.id}: {e}")

    async def _send_heartbeat(self) -> None:
        tasks: List[asyncio.Task] = []
        for idx, peer in enumerate(self.peers):
            task = asyncio.create_task(self.send_heartbeat_to_peer(idx, peer))
            tasks.append(task)
        await asyncio.gather(*tasks)

    def send_heartbeat(self) -> None:
        asyncio.create_task(self._send_heartbeat())

    def give_up_election(self) -> None:
        self.voted_for = None
        self.votes_received = 0

    async def start_election(self) -> None:
        should_request_votes: bool = False
        async with self.lock:
            if self.stop_signal.is_set():
                return
            if self.state != LeaderState and (self.voted_for is None or self.voted_for == self.id):
                await self.set_state(CandidateState)
                self.term += 1
                should_request_votes = True
                self.voted_for = self.id
                self.votes_received = 1
                logger.info(f"{self.id} becomes a candidate and starts election for term {self.term}.")

        if should_request_votes:
            await self.request_votes()

    def reset_election_timer(self) -> None:
        if not self.election_timer or self.election_timeout.done() or self.election_timeout.cancelled():
            self.election_timer = asyncio.create_task(self.election_timer_method())
        self.should_start_election = False

    def state_check(self) -> None:
        if self.state == LeaderState:
            self.send_heartbeat()
        return

    def start_heartbeat(self) -> None:
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = asyncio.create_task(self.heartbeat_timer_method())

    async def _run(self) -> None:
        self._state = FollowerState()
        self._state.on_enter_state(self)
        await self.start_server()

    def run(self, blocking: bool = False, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        logger.info(f"Node {self.id} starting...")
        if blocking:
            if loop is None:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(self._run())
        else:
            def start_loop(loop: Optional[asyncio.AbstractEventLoop]) -> None:
                if not loop:
                    loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self.loop = loop
                loop.run_until_complete(self._run())
            thread = threading.Thread(target=start_loop, args=(loop,))
            thread.start()
            self.threads.append(thread)
    
    async def _stop(self) -> None:
        self.stop_signal.set()
        if self.server:
            self.server.close()

        tasks = [t for t in asyncio.all_tasks(self.loop) if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()

        self._state = FollowerState
        
        await asyncio.gather(*tasks, return_exceptions=True)

    def stop(self) -> None:
        logger.info(f"Node {self.id} stopping")
        loop = self.loop
        if not loop:
            return
        if loop.is_running():
            loop.create_task(self._stop())
        else:
            loop.run_until_complete(self._stop())
        for thread in self.threads:
            thread.join()
        logger.info(f"Node {self.id} stopped")
