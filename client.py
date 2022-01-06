#! /usr/bin/env python3
import yaml
import json
import time
import argparse
import logging
import aiohttp
import asyncio
import hashlib
from aiohttp import web
from random import random




class View:
    def __init__(self, view_number, num_nodes):
        self._view_number = view_number
        self._num_nodes = num_nodes
        self._leader = view_number % num_nodes
    # JSON Encoding.
    def get(self):
        return self._view_number 
    # Read JSON.
    def set_view(self, view):
        self._view_number = view
        self._leader = view % self._num_nodes

class Status:
    def __init__(self, f):
        self.f = f
        self.reply_msgs = {}

    class SequenceElement:
        def __init__(self, proposal):
            self.proposal = proposal
            self.from_nodes = set([])

    def _update_sequence(self, view, proposal, from_node):
        '''
        When message is received, update the record.
        input:
            view: View of own blockchain
            proposal: block proposal.
            from_node: Message sender node
        '''

	# We need to make sure that nodes use the same hash everytime. For this we use hashlib. 
	# Different nodes will send proposals and we need hash in the key to identify the nodes. 
        hash_object = hashlib.md5(json.dumps(proposal, sort_keys=True).encode())
        key = (view.get(), hash_object.digest())
        if key not in self.reply_msgs:
            self.reply_msgs[key] = self.SequenceElement(proposal)
        self.reply_msgs[key].from_nodes.add(from_node)

    def _check_succeed(self):
        '''
        Check for the number of message received if 2f+1 is received then,
        input:
            msg_type: self.PREPARE or self.COMMIT
        '''
        
        for key in self.reply_msgs:
            if len(self.reply_msgs[key].from_nodes)>= self.f + 1:
                return True
        return False

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
    # Deconstructing information from command line.
    parser = argparse.ArgumentParser(description='PBFT Node')
    parser.add_argument('-id', '--client_id', type=int, help='client id')
    parser.add_argument('-nm', '--num_messages', default=10, type=int, help='number of message want to send for this client')
    parser.add_argument('-c', '--config', default='pbft.yaml', type=argparse.FileType('r'), help='use configuration [%(default)s]')
    args = parser.parse_args()
    return args

def conf_parse(conf_file) -> dict:
    '''
    Sample config:

    nodes:
        - host: 
          port:
        - host:
          port:

    loss%:
    block_size:
    skip:

    heartbeat:
        ttl:
        interval:

    election_slice: 10

    sync_interval: 10

    misc:
        network_timeout: 10
    '''
    conf = yaml.load(conf_file)
    return conf

def make_url(node, command):
    return "http://{}:{}/{}".format(node['host'], node['port'], command)

class Client:
    REQUEST = "request"
    REPLY = "reply"
    VIEW_CHANGE_REQUEST = 'view_change_request'

    def __init__(self, conf, args, log):
        self._nodes = conf['nodes']
        self._resend_interval = conf['misc']['network_timeout']
        self._client_id = args.client_id
        self._num_messages = args.num_messages
        self._session = None
        self._address = conf['clients'][self._client_id]
        self._client_url = "http://{}:{}".format(self._address['host'], 
            self._address['port'])
        self._log = log

        self._retry_times = conf['retry_times_before_view_change']
        # Network endurance in case of failure.
        self._f = (len(self._nodes) - 1) // 3

        # New request
        self._is_request_succeed = None
        # Current request status
        self._status = None

    async def request_view_change(self):
        json_data = {
            "action" : "view change"
        }
        for i in range(len(self._nodes)):
            try:
                await self._session.post(make_url(
                    self._nodes[i], Client.VIEW_CHANGE_REQUEST), json=json_data)
            except:
                self._log.info("---> %d failed to send view change message to node %d.", 
                    self._client_id, i)
            else:
                self._log.info("---> %d succeed in sending view change message to node %d.", 
                    self._client_id, i)



    async def get_reply(self, request):
        ''' 
        Verify the replies and count the number of replies received. Accept replies only if the timestamp is valid. Also verify the round number.
        '''
        json_data = await request.json()
        if time.time() - json_data['proposal']['timestamp'] >= self._resend_interval:
            return web.Response()

        view = View(json_data['view'], len(self._nodes))
        self._status._update_sequence(view, json_data['proposal'], json_data['index'])

        if self._status._check_succeed():
            self._is_request_succeed.set()

        return web.Response()


    async def request(self):
        if not self._session:
            timeout = aiohttp.ClientTimeout(self._resend_interval)
            self._session = aiohttp.ClientSession(timeout = timeout)
         
        for i in range(self._num_messages):
            
            accumulate_failure = 0
            is_sent = False
            dest_ind = 0
            self._is_request_succeed = asyncio.Event()
            # Random wait before sending new message.
            await asyncio.sleep(random())
            json_data = {
                'id': (self._client_id, i),
                'client_url': self._client_url + "/" + Client.REPLY,
                'timestamp': time.time(),
                'data': str(i)        
            }

            while 1:
                try:
                    self._status = Status(self._f)
                    await self._session.post(make_url(self._nodes[dest_ind], Client.REQUEST), json=json_data)

                    await asyncio.wait_for(self._is_request_succeed.wait(), self._resend_interval)
                except:
                    
                    json_data['timestamp'] = time.time()
                    self._status = Status(self._f)
                    self._is_request_succeed.clear()
                    self._log.info("---> %d message %d sent fail.", self._client_id, i)

                    accumulate_failure += 1
                    if accumulate_failure == self._retry_times:
                        await self.request_view_change()
                        # Random wait before view_change.
                        await asyncio.sleep(random())
                        accumulate_failure = 0
                        dest_ind = (dest_ind + 1) % len(self._nodes)
                else:
                    self._log.info("---> %d message %d sent successfully.", self._client_id, i)
                    is_sent = True
                if is_sent:
                    break
        await self._session.close()
    

def main():
    logging_config()
    log = logging.getLogger()
    args = arg_parse()
    conf = conf_parse(args.config)
    log.debug(conf)

    addr = conf['clients'][args.client_id]
    log.info("begin")
    

    client = Client(conf, args, log)

    addr = client._address
    host = addr['host']
    port = addr['port']


    asyncio.ensure_future(client.request())

    app = web.Application()
    app.add_routes([
        web.post('/' + Client.REPLY, client.get_reply),
    ])

    web.run_app(app, host=host, port=port, access_log=None)


    
if __name__ == "__main__":
    main()

            
    
