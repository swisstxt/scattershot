#!/usr/bin/env python3

import logging
import argparse
import urllib.parse
import socket
import aiohttp
import asyncio
import concurrent.futures
from functools import partial
import random

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
    """ A custom DNS resolver that returns a static response, choosing one IP randomly from a pool"""

    def __init__(self, staticips, rnd=None):
        super().__init__()
        self.ips = staticips
        if rnd is not None:
            self.rnd = rnd
        else:
            self.rnd = random.Random()

    async def close():
        pass

    async def resolve(self, host, port=None, family=None):
        ip = self.rnd.choice(self.ips)
        log.debug(f"Return IP: {ip}")
        return [{
            "hostname": host,
            "host": ip,
            "port": port,
            "family": family,
            "proto": socket.IPPROTO_TCP,
            "flags": socket.AI_NUMERICHOST | socket.AI_NUMERICSERV,
        }]

class Request(object):
    def __init__(self, url, resolver, method='GET', headers=dict(), readall=True):
        self.method = method
        self.url = url
        self.headers = headers
        self.readall = readall
        self.connector = aiohttp.TCPConnector(resolver=resolver, force_close=True)

    def __str__(self):
        return f"method={self.method} url={self.url} readall={self.readall} headers=[{', '.join(k+': '+v for k, v in self.headers.items())}]"

    def __await__(self):
        return self.send().__await__()

    async def send(self):
        async with aiohttp.ClientSession(connector=self.connector) as session:
            async with session.request(self.method, self.url, headers=self.headers) as response:
                if self.readall:
                    await response.content.read()
                return response

class Scatter(object):
    def __init__(self, url, runtime, parallel=1, rps=1, range=-1, size=-1, ramp=0, ips=None, seed=None, grace=10):
        self.url = url
        self.parallel = parallel
        self.range = range
        self.size = size
        self.ramp = ramp
        self.runtime = runtime
        self.rps = rps
        if type(ips) == str:
            self.ips = ips.split(',')
        else:
            self.ips = ips
        self.rnd = random.Random()
        if seed is not None:
            self.rnd.seed(seed)
        self.grace = grace
        self.resolver = None

    def __str__(self):
        return f"urls={self.url} ips=[{', '.join(self.ips)}] parallel={self.parallel} rps={self.rps} range={self.range}, size={self.size} ramp={self.ramp} runtime={self.runtime}"

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
        self.resolver = CustomResolver(self.ips, self.rnd)
        if self.size < 0:
            result = await Request(self.url, self.resolver, method='HEAD')
            log.debug(f"HEAD request result: {', '.join(result.headers)}")
            if result.headers.get('Accept-Ranges') != "bytes" or 'Content-Length' not in result.headers:
                raise RuntimeError("Server does not support range requests")
            self.size = int(result.headers['Content-Length'])
        if self.range < 0:
            self.range = 0
        if self.range > self.size:
            self.range = self.size

    def makereq(self, delay, sem):
        """Generates a request coroutine that waits for delay seconds and the semaphore, then send out a random request"""
        lower = self.rnd.randint(0, self.size-self.range)
        upper = self.rnd.randint(lower, lower+self.range)
        headers = {
            'Range': f"bytes={lower}-{upper}",
        }
        req = Request(self.url, self.resolver, headers=headers, readall=True)
        async def coro(delay, sem, req):
            await asyncio.sleep(delay)
            log.debug(f"Wall clock: {delay} seconds, sending: {req}")
            async with sem:
                return await req
        return coro(delay, sem, req)

    async def run(self):
        sem = asyncio.Semaphore(self.parallel)
        # logging task
        async def stage(delay, message, level=logging.INFO):
            await asyncio.sleep(delay)
            log.log(level, message)
        tasks = []
        tasks.append(stage(0, f"Wall clock: 0 seconds | Stage: Ramp-up"))
        for step in range(self.ramp):
            prorate = int(self.rps * step / self.ramp)
            log.debug(f"Step: {step} Prorate: {prorate}")
            for index in range(prorate):
                clock = step + index / prorate
                # schedule tasks, each with the calculated wallclock delay
                tasks.append(self.makereq(clock, sem))
        tasks.append(stage(self.ramp, f"Wall clock: {self.ramp} seconds | Stage: Run | Rate: {self.rps} requests/second"))
        for index in range(self.runtime*self.rps):
            clock = self.ramp + index / self.rps
            # schedule tasks, each with the calculated wallclock delay
            tasks.append(self.makereq(clock, sem))
        tasks.append(stage(self.ramp+self.runtime, f"Wall clock: {self.ramp+self.runtime} seconds | Stage: Ramp-down"))
        for step in range(self.ramp):
            prorate = int(self.rps * (1.0 - step / self.ramp))
            log.debug(f"Step: {step} Prorate: {prorate}")
            for index in range(prorate):
                clock = self.ramp + self.runtime + step + index / prorate
                # schedule tasks, each with the calculated wallclock delay
                tasks.append(self.makereq(clock, sem))
        try:
            result = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), self.ramp+self.runtime+self.ramp+self.grace)
        except asyncio.exceptions.TimeoutError:
            log.warning(f"Tasks still running after shutdown and grace period")

async def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--parallel', default=1, type=int, help="Maximum number of parallel requests")
        parser.add_argument('--rps', default=1, type=int, help="Requests to schedule each second")
        parser.add_argument('--range', default=-1, type=int, help="Maximumg size of each range request")
        parser.add_argument('--size', default=-1, type=int, help="Static asset size, send a HEAD request first if unspecified")
        parser.add_argument('--ramp', default=0, type=int, help="Ramp-up and ramp-down time in seconds, rps is scaled up/down in this time period, use 0 to disable ramping")
        parser.add_argument('--runtime', required=True, type=int, help="Test run time in seconds, not counting ramp-up and ramp-down")
        parser.add_argument('--resolve', default=None, type=str, help="List of static target IPs, comma-separated (if not set, the result from a DNS resolution of the first URL will be used)")
        parser.add_argument('--verbose', action="store_true", help="Enable verbose logging")
        parser.add_argument('url', type=str, nargs='+', help="URL to test (multiple URLs are acceptable, they will be tested one-by-one)")
        args = parser.parse_args()

        if args.verbose:
            log.setLevel(logging.DEBUG)
            log.debug("Debug logging enabled")

        for url in args.url:
            tester = Scatter(url=url, parallel=args.parallel, range=args.range, size=args.size, ramp=args.ramp, ips=args.resolve, rps=args.rps, runtime=args.runtime)

            await tester.prepare()
            log.info(f"Starting test with parameters: {tester}")
            results = await tester.run()
            log.info(f"Results: {results}")

    except Exception as e:
        log.exception(e)

if __name__ == '__main__':
    asyncio.run(main())
