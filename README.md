# Scattershot

HTTP range request testing tool.

Hammers a static content server with randomized range requests at a user-defined
rate to observe behavior of the server under load.

## Requirements

Python 3.6, only built-in standard libraries are used.

The tested server/URLs must support range requests.

## Usage

During 60 seconds, send one HTTP request to localhost per second. Only one
requrest may be active at a time:

```shell
$ scattershot --runtime 60 --parallel 1 --rps 1 http://localhost/
```

For more options, see:

```shell
$ scattershot --help
```

## Details

Concurrency is handled by the Python asyncio framework.

This has one serious drawback, however: All requests run in a single thread and
simply yield execution when they are waiting for I/O, which can lead to
contention when there is too much work going on.

## Copyright

Copyright © 2021 SWISS TXT

All rights reserved.
