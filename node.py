#! /usr/bin/env python3
import logging
import argparse
import yaml
import time
from random import random, randint
from collections import Counter
import json
import sys

import asyncio
import aiohttp
from aiohttp import web

import hashlib

VIEW_SET_INTERVAL = 10

class View:
    def __init__(self, view_number, num_nodes):
        self._view_number = view_number
        self._num_nodes = num_nodes
        self._leader = view_number % num_nodes
        # Setting interval to view.
        self._min_set_interval = VIEW_SET_INTERVAL
        self._last_set_time = time.time()

    # Encoding JSON.
    def get_view(self):
        return self._view_number 

    # Read JSON data.
    def set_view(self, view):
        '''
        True if view change success. False otehrwise.
        '''
        if time.time() - self._last_set_time < self._min_set_interval:
            return False
        self._last_set_time = time.time()
        self._view_number = view
        self._leader = view % self._num_nodes
        return True

    def get_leader(self):
        return self._leader

class Status:
    '''
    Status of different phases
    '''
    PREPARE = 'prepare'
    COMMIT = 'commit'
    REPLY = "reply"

    def __init__(self, f):
        self.f = f
        self.request = 0
        self.prepare_msgs = {}     
        self.prepare_certificate = None # Preparing to commit.
        self.commit_msgs = {}
        # No committing if transactions don't match.
        # Commit only if 2f+1 messages have been received.
        self.commit_certificate = None # Preparing to commit.

        # False if not committed.
        self.is_committed = False
    
    class Certificate:
        def __init__(self, view, proposal = 0):
            self._view = view
            self._proposal = proposal

        def to_dict(self):
            return {
                'view': self._view.get_view(),
                'proposal': self._proposal
            }

        def dumps_from_dict(self, dictionary):
            '''
            Update local blockchain copy
            '''
            self._view.set_view(dictionary['view'])
            self._proposal = dictionary['proposal']
        def get_proposal(self):
            return self._proposal


    class SequenceElement:
        def __init__(self, proposal):
            self.proposal = proposal
            self.from_nodes = set([])

    def _update_sequence(self, msg_type, view, proposal, from_node):

        #Update phase status based on message received.
        # Need same hash everytime from node. So we use hashlib.
        # Same hash in the key during the proposal.
        hash_object = hashlib.md5(json.dumps(proposal, sort_keys=True).encode())
        key = (view.get_view(), hash_object.digest())
        if msg_type == Status.PREPARE:
            if key not in self.prepare_msgs:
                self.prepare_msgs[key] = self.SequenceElement(proposal)
            self.prepare_msgs[key].from_nodes.add(from_node)
        elif msg_type == Status.COMMIT:
            if key not in self.commit_msgs:
                self.commit_msgs[key] = self.SequenceElement(proposal)
            self.commit_msgs[key].from_nodes.add(from_node)

    def _check_majority(self, msg_type):
        '''
        Check for the number of messages received is 2f+1.
        '''
        if msg_type == Status.PREPARE:
            if self.prepare_certificate:
                return True
            for key in self.prepare_msgs:
                if len(self.prepare_msgs[key].from_nodes)>= 2 * self.f + 1:
                    return True
            return False

        if msg_type == Status.COMMIT:
            if self.commit_certificate:
                return True
            for key in self.commit_msgs:
                if len(self.commit_msgs[key].from_nodes) >= 2 * self.f + 1:
                    return True
            return False 

class CheckPoint:
    '''
    Check checkpoint status.
    '''
    RECEIVE_CKPT_VOTE = 'receive_ckpt_vote'
    def __init__(self, checkpoint_interval, nodes, f, node_index, 
            lose_rate = 0, network_timeout = 10):
        self._checkpoint_interval = checkpoint_interval
        self._nodes = nodes
        self._f = f
        self._node_index = node_index
        self._loss_rate = lose_rate
        self._log = logging.getLogger(__name__) 
        # Next slot of the given globally accepted checkpoint.
        # For example, the current checkpoint record until slot 99
        # next_slot = 100
        self.next_slot = 0
        # Creating checkpoint
        self.checkpoint = []
        # Validating transactions against checkpoint.
        self._received_votes_by_ckpt = {} 
        self._session = None
        self._network_timeout = network_timeout

        self._log.info("---> %d: Create checkpoint.", self._node_index)

    # Storing transactions for checkpoint creation
    class ReceiveVotes:
        def __init__(self, ckpt, next_slot):
            self.from_nodes = set([])
            self.checkpoint = ckpt
            self.next_slot = next_slot

    def get_commit_upperbound(self):
        '''
        Return the latest checkpoint
        '''
        return self.next_slot + 2 * self._checkpoint_interval

    def _hash_ckpt(self, ckpt):
        '''
        Return hash of checkpoint for verification.
        '''
        hash_object = hashlib.md5(json.dumps(ckpt, sort_keys=True).encode())
        return hash_object.digest()  


    async def receive_vote(self, ckpt_vote):
        '''
        Prepare to receive votes by updating checkpoint status. If 2f+1 votes received only
        then update.
        '''
        self._log.debug("---> %d: Receive checkpoint votes", self._node_index)
        ckpt = json.loads(ckpt_vote['ckpt'])
        next_slot = ckpt_vote['next_slot']
        from_node = ckpt_vote['node_index']

        hash_ckpt = self._hash_ckpt(ckpt)
        if hash_ckpt not in self._received_votes_by_ckpt:
            self._received_votes_by_ckpt[hash_ckpt] = (
                CheckPoint.ReceiveVotes(ckpt, next_slot))
        status = self._received_votes_by_ckpt[hash_ckpt]
        status.from_nodes.add(from_node)
        for hash_ckpt in self._received_votes_by_ckpt:
            if (self._received_votes_by_ckpt[hash_ckpt].next_slot > self.next_slot and 
                    len(self._received_votes_by_ckpt[hash_ckpt].from_nodes) >= 2 * self._f + 1):
                self._log.info("---> %d: Update checkpoint by receiving votes", self._node_index)
                self.next_slot = self._received_votes_by_ckpt[hash_ckpt].next_slot
                self.checkpoint = self._received_votes_by_ckpt[hash_ckpt].checkpoint


    async def propose_vote(self, commit_decisions):
        '''
        Propose next checkpoint when number of transactions exceed the slot value.
        Broadcast new checkpoint to all nodes.
        '''
        proposed_checkpoint = self.checkpoint + commit_decisions
        await self._broadcast_checkpoint(proposed_checkpoint, 
            'vote', CheckPoint.RECEIVE_CKPT_VOTE)


    async def _post(self, nodes, command, json_data):
        '''
        Share JSON data with all nodes in the network.
        '''
        if not self._session:
            timeout = aiohttp.ClientTimeout(self._network_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        for i, node in enumerate(nodes):
            if random() > self._loss_rate:
                self._log.debug("make request to %d, %s", i, command)
                try:
                    _ = await self._session.post(
                        self.make_url(node, command), json=json_data)
                except Exception as e:
                    self._log.error(e)
                    pass

    @staticmethod
    def make_url(node, command):
        return "http://{}:{}/{}".format(node['host'], node['port'], command)

    async def _broadcast_checkpoint(self, ckpt, msg_type, command):
        json_data = {
            'node_index': self._node_index,
            'next_slot': self.next_slot + self._checkpoint_interval,
            'ckpt': json.dumps(ckpt),
            'type': msg_type
        }
        await self._post(self._nodes, command, json_data)

    def get_ckpt_info(self):

        '''
        Synchronize by checking transactions from checkpoint.
        '''
        json_data = {
            'next_slot': self.next_slot,
            'ckpt': json.dumps(self.checkpoint)
        }
        return json_data

    def update_checkpoint(self, json_data):
        '''
        Update new checkpoint when current checkpoint is full.
        '''
        self._log.debug("update_checkpoint: next_slot: %d; update_slot: %d"
            , self.next_slot, json_data['next_slot'])
        if json_data['next_slot'] > self.next_slot:
            self._log.info("---> %d: Update checkpoint by synchronization.", self._node_index)
            self.next_slot = json_data['next_slot']
            self.checkpoint = json.loads(json_data['ckpt'])
        

    async def receive_sync(sync_ckpt):
        '''
        Respond with JSON data of transactions when receiving checkpoint
        synchronization message.
        '''
        self._log.debug("receive_sync in checkpoint: current next_slot:"
            " %d; update to: %d" , self.next_slot, json_data['next_slot'])

        if sync_ckpt['next_slot'] > self._next_slot:
            self.next_slot = sync_ckpt['next_slot']
            self.checkpoint = json.loads(sync_ckpt['ckpt'])

    async def garbage_collection(self):
        '''
        Clear received objects which has number of transactions
        less or equal to current
        '''
        deletes = []
        for hash_ckpt in self._received_votes_by_ckpt:
            if self._received_votes_by_ckpt[hash_ckpt].next_slot <= next_slot:
                deletes.append(hash_ckpt)
        for hash_ckpt in deletes:
            del self._received_votes_by_ckpt[hash_ckpt]


class ViewChangeVotes:
    """
    Verify and record viewchange votes received.
    Find the node with latest checkpoint and perform synchronization.
    """
    def __init__(self, node_index, num_total_nodes):
        # Current round.
        self._node_index = node_index
        # Number of nodes in the network.
        self._num_total_nodes = num_total_nodes
        # Error tolerance value.
        self._f = (self._num_total_nodes - 1) // 3
        # Vote record for primary.
        self.from_nodes = set([])
        # Preparing to designate primary with certificate.
        self.prepare_certificate_by_slot = {}
        self.lastest_checkpoint = None
        self._log = logging.getLogger(__name__)

    def receive_vote(self, json_data):
        '''
        Update information based on votes received.
        Update nodes based on votes received.
        '''
        update_view = None

        prepare_certificates = json_data["prepare_certificates"]

        self._log.debug("%d update prepare_certificate for view %d", 
            self._node_index, json_data['view_number'])

        for slot in prepare_certificates:
            prepare_certificate = Status.Certificate(View(0, self._num_total_nodes))
            prepare_certificate.dumps_from_dict(prepare_certificates[slot])
            # Preparing to designate primary with certificate.
            if slot not in self.prepare_certificate_by_slot or (
                    self.prepare_certificate_by_slot[slot]._view.get_view() < (
                    prepare_certificate._view.get_view())):
                self.prepare_certificate_by_slot[slot] = prepare_certificate

        self.from_nodes.add(json_data['node_index'])


class MPBFTHandler:
    REQUEST = 'request'
    PREPREPARE = 'preprepare'
    PREPARE = 'prepare'
    COMMIT = 'commit'
    REPLY = 'reply'

    NO_OP = 'NOP'

    RECEIVE_SYNC = 'receive_sync'
    RECEIVE_CKPT_VOTE = 'receive_ckpt_vote'

    VIEW_CHANGE_REQUEST = 'view_change_request'
    VIEW_CHANGE_VOTE = "view_change_vote"

    def __init__(self, index, conf):
        self._nodes = conf['nodes']
        self._node_cnt = len(self._nodes)
        self._index = index
        # Error tolerance value.
        self._f = (self._node_cnt - 1) // 3

        # Primary
        self._view = View(0, self._node_cnt)
        self._next_propose_slot = 0

        # Primary check
        if self._index == 0:
            self._is_leader = True
        else:
            self._is_leader = False

        # Checking network
        self._loss_rate = conf['loss%'] / 100

        # Checking network by time.
        self._network_timeout = conf['misc']['network_timeout']

        # Checkpointing

        # After current round, propose new checkpoint for next round.
        self._checkpoint_interval = conf['ckpt_interval']
        self._ckpt = CheckPoint(self._checkpoint_interval, self._nodes, 
            self._f, self._index, self._loss_rate, self._network_timeout)
        # Commit
        self._last_commit_slot = -1

        # Current primary
        self._leader = 0

        # Latest view for synchronization.
        self._follow_view = View(0, self._node_cnt)
        # Synchronization if latest view does not match the local.
        self._view_change_votes_by_view_number = {}
        
        # Key slot alignment with JSON.
        self._status_by_slot = {}

        self._sync_interval = conf['sync_interval']
 
        
        self._session = None
        self._log = logging.getLogger(__name__) 


            
    @staticmethod
    def make_url(node, command):
        return "http://{}:{}/{}".format(node['host'], node['port'], command)

    async def _make_requests(self, nodes, command, json_data):

        #Sending JSON
        resp_list = []
        for i, node in enumerate(nodes):
            if random() > self._loss_rate:
                if not self._session:
                    timeout = aiohttp.ClientTimeout(self._network_timeout)
                    self._session = aiohttp.ClientSession(timeout=timeout)
                self._log.debug("make request to %d, %s", i, command)
                try:
                    resp = await self._session.post(self.make_url(node, command), json=json_data)
                    resp_list.append((i, resp))
                    
                except Exception as e:
                    self._log.error(e)
                    pass
        return resp_list 

    async def _make_response(self, resp):
        '''
        Avoid response clash.
        '''
        if random() < self._loss_rate:
            await asyncio.sleep(self._network_timeout)
        return resp

    async def _post(self, nodes, command, json_data):
        '''
        Share JSON data based on command.
        '''
        if not self._session:
            timeout = aiohttp.ClientTimeout(self._network_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        for i, node in enumerate(nodes):
            if random() > self._loss_rate:
                self._log.debug("make request to %d, %s", i, command)
                try:
                    _ = await self._session.post(self.make_url(node, command), json=json_data)
                except Exception as e:
                    self._log.error(e)
                    pass

    def _legal_slot(self, slot):
        '''
        Verify if the transaction is between upper and lower limit.
        '''
        if int(slot) < self._ckpt.next_slot or int(slot) >= self._ckpt.get_commit_upperbound():
            return False
        else:
            return True

    async def preprepare(self, json_data):
        '''
        Prepare JSON data for commit.
        '''

        
        this_slot = str(self._next_propose_slot)
        self._next_propose_slot = int(this_slot) + 1

        self._log.info("---> %d: on preprepare, propose at slot: %d", 
            self._index, int(this_slot))

        if this_slot not in self._status_by_slot:
            self._status_by_slot[this_slot] = Status(self._f)
        self._status_by_slot[this_slot].request = json_data

        preprepare_msg = {
            'leader': self._index,
            'view': self._view.get_view(),
            'proposal': {
                this_slot: json_data
            },
            'type': 'preprepare'
        }
        
        await self._post(self._nodes, MPBFTHandler.PREPARE, preprepare_msg)



    async def get_request(self, request):
        '''
        Send request to primary if received from other nodes.
        '''
        self._log.info("---> %d: on request", self._index)

        if not self._is_leader:
            if self._leader != None:
                raise web.HTTPTemporaryRedirect(self.make_url(
                    self._nodes[self._leader], MPBFTHandler.REQUEST))
            else:
                raise web.HTTPServiceUnavailable()
        else:
            json_data = await request.json()
            await self.preprepare(json_data)
            return web.Response()

    async def prepare(self, request):
        '''
        Broadcast prpare message once all transactions are verified.
        '''
        json_data = await request.json()

        if json_data['view'] < self._follow_view.get_view():
            # If the proposed transaction block matches with the current transaction, do nothing
            return web.Response()

        self._log.info("---> %d: receive preprepare msg from %d", 
            self._index, json_data['leader'])

        self._log.info("---> %d: on prepare", self._index)
        for slot in json_data['proposal']:

            if not self._legal_slot(slot):
                continue

            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)

            prepare_msg = {
                'index': self._index,
                'view': json_data['view'],
                'proposal': {
                    slot: json_data['proposal'][slot]
                },
                'type': Status.PREPARE
            }
            await self._post(self._nodes, MPBFTHandler.COMMIT, prepare_msg)
        return web.Response()

    async def commit(self, request):
        '''
        Cound number of messages received.
        If 2f+1 messages received send prepare.
        '''
        json_data = await request.json()
        self._log.info("---> %d: receive prepare msg from %d", 
            self._index, json_data['index'])


        if json_data['view'] < self._follow_view.get_view():
            # If the proposed transaction block matches with the current transaction, do nothing
            return web.Response()


        self._log.info("---> %d: on commit", self._index)
        
        for slot in json_data['proposal']:
            if not self._legal_slot(slot):
                continue

            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)
            status = self._status_by_slot[slot]

            view = View(json_data['view'], self._node_cnt)

            status._update_sequence(json_data['type'], 
                view, json_data['proposal'][slot], json_data['index'])

            if status._check_majority(json_data['type']):
                status.prepare_certificate = Status.Certificate(view, 
                    json_data['proposal'][slot])
                commit_msg = {
                    'index': self._index,
                    'view': json_data['view'],
                    'proposal': {
                        slot: json_data['proposal'][slot]
                    },
                    'type': Status.COMMIT
                }
                await self._post(self._nodes, MPBFTHandler.REPLY, commit_msg)
        return web.Response()

    async def reply(self, request):
        '''
        Commit if count of message received is 2f+1.
        '''
        
        json_data = await request.json()
        self._log.info("---> %d: on reply", self._index)

        if json_data['view'] < self._follow_view.get_view():
            # If the proposed transaction block matches with the current transaction, do nothing
            return web.Response()


        self._log.info("---> %d: receive commit msg from %d", 
            self._index, json_data['index'])

        for slot in json_data['proposal']:
            if not self._legal_slot(slot):
                continue

            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)
            status = self._status_by_slot[slot]

            view = View(json_data['view'], self._node_cnt)

            status._update_sequence(json_data['type'], 
                view, json_data['proposal'][slot], json_data['index'])

            # Check for 2f+1 votes before commit.
            if not status.commit_certificate and status._check_majority(json_data['type']):
                status.commit_certificate = Status.Certificate(view, 
                    json_data['proposal'][slot])

                self._log.debug("Add commit certifiacte to slot %d", int(slot))
                
                # Reply after commit success.
                if self._last_commit_slot == int(slot) - 1 and not status.is_committed:

                    reply_msg = {
                        'index': self._index,
                        'view': json_data['view'],
                        'proposal': json_data['proposal'][slot],
                        'type': Status.REPLY
                    }
                    status.is_committed = True
                    self._last_commit_slot += 1

                    # After successful commit add next checkpoint,
                    if (self._last_commit_slot + 1) % self._checkpoint_interval == 0:
                        await self._ckpt.propose_vote(self.get_commit_decisions())
                        self._log.info("---> %d: Propose checkpoint with last slot: %d. "
                            "In addition, current checkpoint's next_slot is: %d", 
                            self._index, self._last_commit_slot, self._ckpt.next_slot)


                    # Committing!
                    await self._commit_action()
                    try:
                        await self._session.post(
                            json_data['proposal'][slot]['client_url'], json=reply_msg)
                    except:
                        self._log.error("Send message failed to %s", 
                            json_data['proposal'][slot]['client_url'])
                        pass
                    else:
                        self._log.info("%d reply to %s successfully!!", 
                            self._index, json_data['proposal'][slot]['client_url'])
                
        return web.Response()

    def get_commit_decisions(self):
        '''
        Verify checkpoint for transactions and perform commit.
        '''
        commit_decisions = []
        for i in range(self._ckpt.next_slot, self._last_commit_slot + 1):
            status = self._status_by_slot[str(i)]
            proposal = status.commit_certificate._proposal
            commit_decisions.append((proposal['id'], proposal['data']))

        return commit_decisions

    async def _commit_action(self):
        '''
        Write commit to disk.
        '''
        with open("{}.dump".format(self._index), 'w') as f:
            dump_data = self._ckpt.checkpoint + self.get_commit_decisions()
            json.dump(dump_data, f)

    async def receive_ckpt_vote(self, request):
        '''
        Message CheckPoint.propose_vote()
        '''
        self._log.info("---> %d: receive checkpoint vote.", self._index)
        json_data = await request.json()
        await self._ckpt.receive_vote(json_data)
        return web.Response()

    async def receive_sync(self, request):
        '''
        Update local copy of blockchain with sync messages using checkpoints.
        '''
        self._log.info("---> %d: on receive sync stage.", self._index)
        json_data = await request.json()
        self._ckpt.update_checkpoint(json_data['checkpoint'])
        self._last_commit_slot = max(self._last_commit_slot, self._ckpt.next_slot - 1)

        for slot in json_data['commit_certificates']:
            # Check for transactions where update is required.
            if int(slot) >= self._ckpt.get_commit_upperbound() or (
                    int(slot) < self._ckpt.next_slot):
                continue

            certificate = json_data['commit_certificates'][slot]
            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)
                commit_certificate = Status.Certificate(View(0, self._node_cnt))
                commit_certificate.dumps_from_dict(certificate)
                self._status_by_slot[slot].commit_certificate =  commit_certificate
            elif not self._status_by_slot[slot].commit_certificate:
                commit_certificate = Status.Certificate(View(0, self._node_cnt))
                commit_certificate.dumps_from_dict(certificate)
                self._status_by_slot[slot].commit_certificate =  commit_certificate

        # Commit when transactions are confirmed based on previous round.
        while (str(self._last_commit_slot + 1) in self._status_by_slot and 
                self._status_by_slot[str(self._last_commit_slot + 1)].commit_certificate):
            self._last_commit_slot += 1

            # Create a new checkpoint once commit success.
            if (self._last_commit_slot + 1) % self._checkpoint_interval == 0:
                await self._ckpt.propose_vote(self.get_commit_decisions())

                self._log.info("---> %d: During rev_sync, Propose checkpoint with l "
                    "ast slot: %d. In addition, current checkpoint's next_slot is: %d", 
                    self._index, self._last_commit_slot, self._ckpt.next_slot)

        await self._commit_action()

        return web.Response()
        

    async def synchronize(self):
        '''
        Once committed broadcast the current checkpoint with latest details.
        '''
        while 1:
            await asyncio.sleep(self._sync_interval)
            commit_certificates = {}
            for i in range(self._ckpt.next_slot, self._ckpt.get_commit_upperbound()):
                slot = str(i)
                if (slot in self._status_by_slot) and (
                        self._status_by_slot[slot].commit_certificate):
                    status = self._status_by_slot[slot]
                    commit_certificates[slot] = status.commit_certificate.to_dict()
            json_data = {
                'checkpoint': self._ckpt.get_ckpt_info(),
                'commit_certificates':commit_certificates
            }
            await self._post(self._nodes, MPBFTHandler.RECEIVE_SYNC, json_data)

    async def get_prepare_certificates(self):
        '''
        Get information of commit for viewchange.
        '''
        prepare_certificate_by_slot = {}
        for i in range(self._ckpt.next_slot, self._ckpt.get_commit_upperbound()):
            slot = str(i)
            if slot in self._status_by_slot:
                status = self._status_by_slot[slot]
                if status.prepare_certificate:
                    prepare_certificate_by_slot[slot] = (
                        status.prepare_certificate.to_dict())
        return prepare_certificate_by_slot

    async def _post_view_change_vote(self):
        '''
        Prepare for view change by broadcasting view change message
        with all necessary information
        '''
        view_change_vote = {
            "node_index": self._index,
            "view_number": self._follow_view.get_view(),
            "checkpoint":self._ckpt.get_ckpt_info(),
            "prepare_certificates":await self.get_prepare_certificates(),

        }
        await self._post(self._nodes, MPBFTHandler.VIEW_CHANGE_VOTE, view_change_vote)

    async def get_view_change_request(self, request):
        '''
        Receive request for view change from client. Broadcast vote message for view
        change with all necessary information.
        '''

        self._log.info("---> %d: receive view change request from client.", self._index)
        json_data = await request.json()
        # Verify message for validity.
        if json_data['action'] != "view change":
            return web.Response()
        # Update round number and wait for updateview message.
        if not self._follow_view.set_view(self._follow_view.get_view() + 1):
            return web.Response()

        self._leader = self._follow_view.get_leader()
        if self._is_leader:
            self._log.info("%d is not leader anymore. View number: %d", 
                    self._index, self._follow_view.get_view())
            self._is_leader = False

        self._log.debug("%d: vote for view change to %d.", 
            self._index, self._follow_view.get_view())

        await self._post_view_change_vote()

        return web.Response()

    async def receive_view_change_vote(self, request):
        '''
        Receive message for view change vote. Update local blockchain if received
        mesage has highter checkpoint value than current local checkpoint value.
        Count the number of view change messages.
        '''

        self._log.info("%d receive view change vote.", self._index)
        json_data = await request.json()
        view_number = json_data['view_number']
        if view_number not in self._view_change_votes_by_view_number:
            self._view_change_votes_by_view_number[view_number]= (
                ViewChangeVotes(self._index, self._node_cnt))


        self._ckpt.update_checkpoint(json_data['checkpoint'])
        self._last_commit_slot = max(self._last_commit_slot, self._ckpt.next_slot - 1)

        votes = self._view_change_votes_by_view_number[view_number]

        votes.receive_vote(json_data)

        #Check for number of message received and proclaim as primary if 2f+1 messages received.

        if len(votes.from_nodes) >= 2 * self._f + 1:

            if self._follow_view.get_leader() == self._index and not self._is_leader:

                self._log.info("%d: Change to be leader!! view_number: %d", 
                    self._index, self._follow_view.get_view())

                self._is_leader = True
                self._view.set_view(self._follow_view.get_view())
                last_certificate_slot = max(
                    [int(slot) for slot in votes.prepare_certificate_by_slot] + [-1])

                self._next_propose_slot = last_certificate_slot + 1

                proposal_by_slot = {}
                for i in range(self._ckpt.next_slot, last_certificate_slot + 1):
                    slot = str(i)
                    if slot not in votes.prepare_certificate_by_slot:

                        self._log.debug("%d decide no_op for slot %d", 
                            self._index, int(slot))

                        proposal = {
                            'id': (-1, -1),
                            'client_url': "no_op",
                            'timestamp':"no_op",
                            'data': MPBFTHandler.NO_OP
                        }
                        proposal_by_slot[slot] = proposal
                    elif not self._status_by_slot[slot].commit_certificate:
                        proposal = votes.prepare_certificate_by_slot[slot].get_proposal()
                        proposal_by_slot[slot] = proposal

                await self.fill_bubbles(proposal_by_slot)
        return web.Response()

    async def fill_bubbles(self, proposal_by_slot):
        '''
        Synchronize local blockhain copy with view change message.
        '''
        self._log.info("---> %d: on fill bubbles.", self._index)
        self._log.debug("Number of bubbles: %d", len(proposal_by_slot))

        bubbles = {
            'leader': self._index,
            'view': self._view.get_view(),
            'proposal': proposal_by_slot,
            'type': 'preprepare'
        }
        
        await self._post(self._nodes, MPBFTHandler.PREPARE, bubbles)

    async def garbage_collection(self):
        '''
        Update local blockchain if current checkpoint value is less than the
        received checkpoint value.
        '''
        await asyncio.sleep(self._sync_interval)
        delete_slots = []
        for slot in self._status_by_slot:
            if int(slot) < self._ckpt.next_slot:
                delete_slots.append(slot)
        for slot in delete_slots:
            del self._status_by_slot[slot]

        # Free up memory
        await self._ckpt.garbage_collection()



def logging_config(log_level=logging.INFO, log_file=None):
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        return

    root_logger.setLevel(log_level)

    f = logging.Formatter("[%(levelname)s]%(module)s->%(funcName)s: \t %(message)s \t --- %(asctime)s")

    h = logging.StreamHandler()
    h.setFormatter(f)
    h.setLevel(log_level)
    root_logger.addHandler(h)

    if log_file:
        from logging.handlers import TimedRotatingFileHandler
        h = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=7)
        h.setFormatter(f)
        h.setLevel(log_level)
        root_logger.addHandler(h)

def arg_parse():
    parser = argparse.ArgumentParser(description='MPBFT Node')
    parser.add_argument('-i', '--index', type=int, help='node index')
    parser.add_argument('-c', '--config', default='mpbft.yaml', type=argparse.FileType('r'), help='use configuration [%(default)s]')
    parser.add_argument('-lf', '--log_to_file', default=False, type=bool, help='Whether to dump log messages to file, default = False')
    args = parser.parse_args()
    return args

def conf_parse(conf_file) -> dict:
    conf = yaml.load(conf_file)
    return conf

def main():
    args = arg_parse()
    if args.log_to_file:
        logging.basicConfig(filename='log_' + str(args.index),
                            filemode='a', level=logging.DEBUG)
    logging_config()
    log = logging.getLogger()
    conf = conf_parse(args.config)
    log.debug(conf)

    addr = conf['nodes'][args.index]
    host = addr['host']
    port = addr['port']

    mpbft = MPBFTHandler(args.index, conf)

    asyncio.ensure_future(mpbft.synchronize())
    asyncio.ensure_future(mpbft.garbage_collection())

    app = web.Application()
    app.add_routes([
        web.post('/' + MPBFTHandler.REQUEST, mpbft.get_request),
        web.post('/' + MPBFTHandler.PREPREPARE, mpbft.preprepare),
        web.post('/' + MPBFTHandler.PREPARE, mpbft.prepare),
        web.post('/' + MPBFTHandler.COMMIT, mpbft.commit),
        web.post('/' + MPBFTHandler.REPLY, mpbft.reply),
        web.post('/' + MPBFTHandler.RECEIVE_CKPT_VOTE, mpbft.receive_ckpt_vote),
        web.post('/' + MPBFTHandler.RECEIVE_SYNC, mpbft.receive_sync),
        web.post('/' + MPBFTHandler.VIEW_CHANGE_REQUEST, mpbft.get_view_change_request),
        web.post('/' + MPBFTHandler.VIEW_CHANGE_VOTE, mpbft.receive_view_change_vote),
        ])

    web.run_app(app, host=host, port=port, access_log=None)


if __name__ == "__main__":
    main()

