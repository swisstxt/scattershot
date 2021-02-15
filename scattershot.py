#!/usr/bin/env python3

import logging
import argparse
import threading
import urllib.parse
import socket
import http.client
import aiohttp
import asyncio

logging.basicConfig()
log = logging.getLogger(name="scattershot")
log.setLevel(logging.INFO)

class Counter(object):
    """A lock-protected atomic counter"""
    def __init__(self, initial=0):
        self.value = initial
        # a non-reentrant lock should suffice
        self.lock = threading.Lock()
    def inc(self):
        with self.lock:
            self.value += 1
    def dec(self):
        with self.lock:
            self.value -= 1
    def get(self):
        with self.lock:
            ret = self.value
        return ret

class Request(object):
    def __init__(self, protocol, host, port, url, method='GET', range=None, headers=dict()):
        if protocol == 'http':
            self.conn = http.client.HTTPConnection(host, port)
        elif protocol == 'https':
            self.conn = http.client.HTTPSConnection(host, port)
        else:
            raise ValueError(f"Invalid protocol {protocol}")
        self.method = method
        self.url = url
        self.range = range
        self.headers = headers

    def send(self):
        try:
            self.conn.connect()
            self.conn.putrequest(self.method, self.url)
            if self.range is not None:
                self.conn.putheader(f"Range: {self.range[0]},{self.range[1]}")
            for k, v in self.headers:
                self.conn.putheader(f"{k}: {v}")
            self.endheaders()
            with self.conn.getresponse() as response:
                log.debug(f"status={response.getcode()}")
                reponse.read()
        finally:
            self.conn.close()

class Scatter(object):
    def __init__(self, urls=[], parallel=1, rps=1, minrange=-1, maxrange=-1, size=-1, ramp=0, ips=None):
        if type(urls) == str:
            self.urls = [urls]
        else:
            self.urls = urls
        self.parallel = parallel
        self.range = (minrange, maxrange)
        self.size = size
        self.ramp = ramp
        if type(ips) == str:
            self.ips = ips.split(',')
        else:
            self.ips = ips

    def __str__(self):
        return f"urls=[{', '.join(self.urls)}] protocol={self.protocol} ips=[{', '.join(self.ips)}] port={self.port} parallel={self.parallel} range={self.range[0]}..{self.range[1]}, size={self.size} ramp={self.ramp}"

    def prepare(self):
        target = urllib.parse.urlparse(self.urls[0])
        self.protocol = target.scheme
        hostport = target.netloc.split(':')
        if len(hostport) > 1:
            self.port = hostport[1]
        else:
            self.port = self.protocol
        if self.ips is None:
            self.ips = [addr[4][0] for addr in socket.getaddrinfo(hostport[0], self.port, proto=socket.IPPROTO_TCP)]
        if self.size == -1:
            Request(self.protocol, self.ips[0], self.port, self.urls[0], method='HEAD').send()
            # TODO send HEAD
            self.size=1000
        if self.range[0] < 0 or self.range[0] > self.size:
            self.range = (self.size, self.range[1])
        if self.range[1] < 0 or self.range[1] > self.size:
            self.range = (self.range[0], self.size)
        if self.range[1] < self.range[0]:
            self.range = (self.range[0], self.range[0])

    def run(self):
        return None

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--parallel', default=1, type=int, help="Maximum number of parallel requests")
        parser.add_argument('--rps', default=1, type=int, help="Maximum number of requests per second")
        parser.add_argument('--minrange', default=-1, type=int, help="Minimum range length, use -1 to set to asset size")
        parser.add_argument('--maxrange', default=-1, type=int, help="Maximum range length, use -1 to set to asset size")
        parser.add_argument('--size', default=-1, type=int, help="Static asset size, use -1 to send a HEAD request to determine asset size first")
        parser.add_argument('--ramp', default=0, type=int, help="Ramp-up and ramp-down time in seconds, rps is scaled up/down in this time period, use 0 to disable ramping")
        parser.add_argument('--resolve', default=None, type=str, help="List of static target IPs, comma-separated (if not set, the result from a DNS resolution of the first URL will be used)")
        parser.add_argument('--verbose', action="store_true", help="Enable verbose logging")
        parser.add_argument('url', type=str, nargs='+', help="URL to test (multiple URLs are acceptable, but only the first URL's protocol/port fields will be used, and only the first URL will be resolved)")
        args = parser.parse_args()

        if args.verbose:
            log.setLevel(logging.DEBUG)
            log.debug("Debug logging enabled")

        tester = Scatter(urls=args.url, parallel=args.parallel, minrange=args.minrange, maxrange=args.maxrange, size=args.size, ramp=args.ramp, ips=args.resolve)

        tester.prepare()
        log.info(f"Starting test with parameters: {tester}")
        results = tester.run()
        log.info(f"Results: {results}")

    except Exception as e:
        log.exception(e)
