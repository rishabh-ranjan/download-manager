#!/usr/bin/env python3

import matplotlib.pyplot as plt

import argparse
import asyncio
import collections
import csv
import os
import re
import time
import tqdm

url_re = re.compile(r'http://(?P<hostname>[^/]*)(?P<resource>.*)') # regex for URL
def parse_csv(path): # parse CSV file as list of (hostname, resource, #connections)
    ret = []
    with open(path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            match = url_re.match(row[0])
            n_conn = int(row[1].strip())
            ret.append((match.group('hostname'), match.group('resource'), n_conn))
    return ret

class RangeGen: # generator of byte ranges (chunks) for download
    def __init__(self, total):
        self.total = total
        self.cur = 0
        self.retry_pool = collections.deque()

    def next(self, size):
        if self.cur >= self.total:
            if len(self.retry_pool) == 0:
                return None
            return self.retry_pool.popleft()
        old_cur = self.cur
        self.cur += size
        return (old_cur, min(old_cur+size, self.total)-1)

    def add_from(self, iterable):
        self.retry_pool.extend(iterable)

async def unsure_connection(protocol_factory, hostname, port): # retry connecting till success
    try:
        await loop.create_connection(protocol_factory, hostname, port)
    except:
        tqdm.tqdm.write(f'unable to connect to {hostname}, retry in {args.retry_gap} s.')
        await asyncio.sleep(args.retry_gap)
        await unsure_connection(protocol_factory, hostname, port)

class HeadProtocol(asyncio.Protocol): # to receive the header only
    header_re = re.compile(br'.*\r\nContent-Length: (?P<content_len>\d*).*\r\n\r\n', re.DOTALL) # header regex

    def __init__(self, hostname, port, resource, content_len_future):
        super().__init__()
        self.hostname = hostname
        self.port = port
        self.resource = resource
        self.content_len_future = content_len_future
        self.buffer = b''

    def connection_made(self, transport):
        self.transport = transport
        req = f'HEAD {self.resource} HTTP/1.1\r\nHost: {self.hostname}\r\n\r\n'.encode()
        self.transport.write(req)

    def data_received(self, data):
        self.buffer += data
        if self.buffer.find(b'\r\n\r\n') == -1: # entire header not yet received
            return
        self.content_len_future.set_result(int(HeadProtocol.header_re.match(self.buffer).group('content_len')))
        self.transport.close()

class DownloadProtocol(asyncio.Protocol): # to download on a single connection
    header_re = re.compile(br'.*\r\nContent-Range: bytes (?P<range_start>\d*)-(?P<range_end>\d*).*\r\n\r\n', re.DOTALL) # header regex

    def __init__(self, conn_id, hostname, port, resource, range_gen, tqdm_local, log_dl, log_time):
        super().__init__()
        self.conn_id = conn_id
        self.hostname = hostname
        self.port = port
        self.resource = resource
        self.range_gen = range_gen
        self.tqdm_local = tqdm_local
        self.pipe = collections.deque()
        self.buffer = b''
        self.header_recvd = False
        self.log_dl = log_dl
        self.log_time = log_time
        self.alive = True
        loop.call_later(args.timeout, self.check_connection)

    def check_connection(self): # trampoline to check connection
        if self.alive:
            self.alive = False
            loop.call_later(args.timeout, self.check_connection)
        else:
            tqdm.tqdm.write(f'closing connection #{self.conn_id} on timeout')
            self.transport.close()

    def connection_lost(self, exc):
        tqdm.tqdm.write(f'\033[31mconnection #{self.conn_id} ({self.hostname}) lost! {exc if exc else ""}\033[0m')
        self.range_gen.add_from(self.pipe)
        if len(range_gen.retry_pool) != 0:
            loop.create_task(unsure_connection(make_factory(DownloadProtocol, self.conn_id, self.hostname, self.port, self.resource, self.range_gen, self.tqdm_local, self.log_dl, self.log_time), self.hostname, self.port)) # schedule task to reconnect

    def connection_made(self, transport):
        self.alive = True
        tqdm.tqdm.write(f'\033[32mconnection #{self.conn_id} ({self.hostname}) established.\033[0m')
        self.transport = transport
        for _ in range(args.pipeline):
            self.send_request()
    
    def send_request(self):
        self.alive = True
        new_range = range_gen.next(args.chunk_size)
        if new_range is None: # no task available
            return
        self.pipe.append(new_range)
        range_start, range_end = new_range
        req = (
                f'GET {self.resource} HTTP/1.1\r\n'
                f'Host: {self.hostname}\r\n'
                f'Connection: keep-alive\r\n'
                f'Range: bytes={range_start}-{range_end}\r\n'
                f'\r\n'
                ).encode()
        self.transport.write(req)

    def data_received(self, data):
        global remains

        self.alive = True
        self.buffer += data

        if not self.header_recvd:
            header_delim = self.buffer.find(b'\r\n\r\n')
            if header_delim == -1: # entire header not yet received
                return
            self.range_start, self.range_end = self.pipe[0]
            self.range_len = self.range_end-self.range_start+1
            self.buffer = self.buffer[header_delim+4:]
            self.header_recvd = True

        if len(self.buffer) >= self.range_len: # entire chunk received
            self.log_dl.append(self.log_dl[-1]+self.range_len)
            self.log_time.append(time.time() - ref_time)
            outfile.seek(self.range_start)
            outfile.write(self.buffer[:self.range_len]) # write chunk to file
            self.pipe.popleft()
            tqdm_global.update(self.range_len)
            self.tqdm_local.update(self.range_len)
            remains -= self.range_len
            if remains == 0: # entire download finished
                done_future.set_result(True)
            else:
                # push new request to pipeline
                self.buffer = self.buffer[self.range_len:]
                self.header_recvd = False
                self.send_request()

def make_factory(class_obj, *args, **kwargs):
    return lambda: class_obj(*args, **kwargs)

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('csv_path', help='input CSV file with URLs and number of parallel TCP connections to use')
    arg_parser.add_argument('out_path', help='output file for download')
    arg_parser.add_argument('-c', '--chunk_size', type=int, default=10_000, help='chunk size in bytes')
    arg_parser.add_argument('-p', '--pipeline', type=int, default=5, help='number of GET requests to pipeline')
    arg_parser.add_argument('-t', '--timeout', type=float, default=5.0, help='TCP timeout')
    arg_parser.add_argument('-r', '--retry_gap', type=float, default=1.0, help='time to retry connection after')
    arg_parser.add_argument('-s', '--plot_path', default=None, help='draw and store plot in path')
    args = arg_parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.set_debug(True)

    conn_list = parse_csv(args.csv_path)
    hostname, resource, _ = conn_list[0]
    port = 80

    content_len_future = loop.create_future()
    loop.create_task(unsure_connection(make_factory(HeadProtocol, hostname, port, resource, content_len_future), hostname, port))
    loop.run_until_complete(content_len_future)
    content_len = content_len_future.result()
    range_gen = RangeGen(content_len)
    tqdm_global = tqdm.tqdm(desc='total', position=0, total=content_len, unit='B', unit_scale=True)

    outfile = open(args.out_path, 'wb')
    outfile.write(os.urandom(content_len)) # pre-allocation for efficient disk IO

    all_log_dl = [] # log download data for each connection
    all_log_time = [] # log time data for each connection
    all_hostnames = []
    ref_time = time.time()

    conn_id = 0
    done_future = loop.create_future()
    remains = content_len
    for hostname, resource, n_conn in conn_list:
        for _ in range(n_conn):
            conn_id += 1
            all_log_dl.append([0])
            all_log_time.append([0])
            all_hostnames.append(hostname)
            tqdm_local = tqdm.tqdm(desc=f'{f"{conn_id: >2}> {hostname}": <21}', position=conn_id, total=content_len, unit='B', unit_scale=True)
            loop.create_task(unsure_connection(make_factory(DownloadProtocol, conn_id, hostname, port, resource, range_gen, tqdm_local, all_log_dl[-1], all_log_time[-1]), hostname, port))
    
    loop.run_until_complete(done_future)

    for _ in range(conn_id+1): # move cursor beyond tqdm_local's
        print()

    print(f'total download time: {time.time() - ref_time} s')

    if args.plot_path:
        # plot data recvd on connection vs time
        for i in range(conn_id):
            plt.plot(all_log_time[i], all_log_dl[i], label=f'#{i} ({all_hostnames[i]})')
        plt.legend()
        plt.xlabel('time from connection (s)')
        plt.ylabel('data downloaded (bytes)')
        plt.title('data vs time for different TCP connections')
        plt.savefig(args.plot_path)
        print(f'plot saved in {args.plot_path}')
