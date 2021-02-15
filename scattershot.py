#!/usr/bin/env python3

import logging
import argparse
import urllib.parse
import socket
import aiohttp
import asyncio
import concurrent.futures
from functools import partial

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

class CustomResolver(aiohttp.abc.AbstractResolver):
    """ A custom DNS resolver that always returns the same address"""

    def __init__(self, staticip):
        self.ip = staticip

    async def close():
        pass

    async def resolve(self, host, port=None, family=None):
        return [{
            "hostname": host,
            "host": self.ip,
            "port": port,
            "family": family,
            "proto": socket.IPPROTO_TCP,
            "flags": socket.AI_NUMERICHOST | socket.AI_NUMERICSERV,
        }]

class Request(object):
    def __init__(self, url, method='GET', headers=dict(), ip=None, readall=True):
        self.method = method
        self.url = url
        self.headers = headers
        self.readall = readall
        self.resolver = CustomResolver(ip)

    def __await__(self):
        return self.send().__await__()

    async def send(self):
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver=self.resolver)) as session:
            async with session.request(self.method, self.url, headers=self.headers) as response:
                if self.readall:
                    await response.content.read()
                return response

class Scatter(object):
    def __init__(self, url, runtime, parallel=1, rps=1, minrange=-1, maxrange=-1, size=-1, ramp=0, ips=None):
        self.url = url
        self.parallel = parallel
        self.range = (minrange, maxrange)
        self.size = size
        self.ramp = ramp
        self.runtime = runtime
        self.rps = rps
        if type(ips) == str:
            self.ips = ips.split(',')
        else:
            self.ips = ips

    def __str__(self):
        return f"urls={self.url} ips=[{', '.join(self.ips)}] parallel={self.parallel} rps={self.rps} range={self.range[0]}..{self.range[1]}, size={self.size} ramp={self.ramp} runtime={self.runtime}"

    async def prepare(self):
        if self.ips is None:
            target = urllib.parse.urlparse(self.url)
            hostport = target.netloc.split(':')
            if len(hostport) > 1:
                host = hostport[0]
                port = hostport[1]
            else:
                host = hostport[0]
                port = None
            # TODO use asyncio.get_event_loop().getaddrinfo() instead
            self.ips = [addr[4][0] for addr in socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)]
        if self.size == -1:
            result = await Request(self.url, method='HEAD', ip=self.ips[0])
            if result.headers.get('Accept-Ranges') != "bytes" or 'Content-Length' not in result.headers:
                raise RuntimeError("Server does not support range requests")
            self.size = int(result.headers['Content-Length'])
        if self.range[0] < 0 or self.range[0] > self.size:
            self.range = (self.size, self.range[1])
        if self.range[1] < 0 or self.range[1] > self.size:
            self.range = (self.range[0], self.size)
        if self.range[1] < self.range[0]:
            self.range = (self.range[0], self.range[0])

    async def run(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.parallel) as executor:
            headers = {
                'Range': f"bytes={self.range[0]}-{self.range[1]}",
            }
            loop = asyncio.get_event_loop()
            start = loop.time()
            counter = start
            log.info(f"Stage: Ramp-up")
            for step in range(self.ramp):
                log.debug(f"Wall clock: {counter} seconds")
                tasks = [loop.run_in_executor(executor, Request(self.url, headers=headers, ip=self.ip, readall=True)) for step in range(step)]
                await asyncio.wait_for(asyncio.gather(tasks, return_exceptions=True), 1)
                counter += 1
            log.info(f"Stage: Run")
            for step in range(self.runtime):
                log.debug(f"Wall clock: {counter} seconds")
                tasks = [loop.run_in_executor(executor, Request(self.url, headers=headers, ip=self.ip, readall=True)) for step in range(self.rps)]
                await asyncio.wait_for(asyncio.gather(tasks, return_exceptions=True), 1)
                counter += 1
            log.info(f"Stage: Ramp-down")
            for step in range(self.ramp -1 , -1, -1):
                log.debug(f"Wall clock: {counter} seconds")
                tasks = [loop.run_in_executor(executor, Request(self.url, headers=headers, ip=self.ip, readall=True)) for step in range(step)]
                await asyncio.wait_for(asyncio.gather(tasks, return_exceptions=True), 1)
                counter += 1

            # force shutdown, drop all pending requests in case we scheduled more than permitted, but postpone wait
            executor.shutdown(cancel_futures=True)

async def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--parallel', default=1, type=int, help="Maximum number of parallel requests")
        parser.add_argument('--rps', default=1, type=int, help="Maximum number of requests per second")
        parser.add_argument('--minrange', default=-1, type=int, help="Minimum range length, use -1 to set to asset size")
        parser.add_argument('--maxrange', default=-1, type=int, help="Maximum range length, use -1 to set to asset size")
        parser.add_argument('--size', default=-1, type=int, help="Static asset size, use -1 to send a HEAD request to determine asset size first")
        parser.add_argument('--ramp', default=0, type=int, help="Ramp-up and ramp-down time in seconds, rps is scaled up/down in this time period, use 0 to disable ramping")
        parser.add_argument('--runtime', type=int, help="Test run time in seconds, not counting ramp-up and ramp-down")
        parser.add_argument('--resolve', default=None, type=str, help="List of static target IPs, comma-separated (if not set, the result from a DNS resolution of the first URL will be used)")
        parser.add_argument('--verbose', action="store_true", help="Enable verbose logging")
        parser.add_argument('url', type=str, nargs='+', help="URL to test (multiple URLs are acceptable, they will be tested one-by-one)")
        args = parser.parse_args()

        if args.verbose:
            log.setLevel(logging.DEBUG)
            log.debug("Debug logging enabled")

        for url in args.url:
            tester = Scatter(url=url, parallel=args.parallel, minrange=args.minrange, maxrange=args.maxrange, size=args.size, ramp=args.ramp, ips=args.resolve, rps=args.rps, runtime=args.runtime)

            await tester.prepare()
            log.info(f"Starting test with parameters: {tester}")
            results = await tester.run()
            log.info(f"Results: {results}")

    except Exception as e:
        log.exception(e)

if __name__ == '__main__':
    asyncio.run(main())
