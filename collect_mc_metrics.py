#!/usr/bin/env python3
"""
Memcached/repcached metrics for monitoring. 
"""
import json
import logging
import re
import socket
import subprocess
import telnetlib
import time
import typing

import requests

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
hdl = logging.StreamHandler()
log_fmt = '[%(levelname)s] %(asctime)s PID:%(process)d %(name)s %(filename)s:%(lineno)d %(message)s'
hdl.setFormatter(fmt=logging.Formatter(fmt=log_fmt))
logger.addHandler(hdl)


class MemcachedStats:
    bin_name = 'memcached'

    _client = None
    _key_regex = re.compile('ITEM (.*) \[(.*); (.*)\]')
    _slab_regex = re.compile('STAT items:(.*):number')
    _stat_regex = re.compile("STAT (.*) ([0-9]+\.?[0-9]*)\r")

    def __init__(self, host='localhost', port=11211, timeout=2.0):
        self._host = host
        self._port = int(port)
        self._timeout = timeout

    @staticmethod
    def get_instances() -> set:
        result = set()
        ls_procs = ''' ps -ef |grep {bin_name}|grep -v grep '''.format(
            bin_name=MemcachedStats.bin_name,
        )
        lines = subprocess.getoutput(ls_procs).split("\n")
        for line in lines:
            line = line.strip()
            m = re.search(pattern="\-p\s+(?P<port>\d+)", string=line, flags=re.IGNORECASE)
            if m:
                port = m.group("port")
                result.add(int(port))
        return result

    @property
    def client(self) -> telnetlib.Telnet:
        if self._client is None:
            msg = "create connection %s:%s" % (self._host, self._port)
            logging.debug(msg)
            self._client = telnetlib.Telnet(
                host=self._host,
                port=self._port,
                timeout=self._timeout,
            )
        return self._client

    def command(self, cmd: str) -> str:
        """Write a command to telnet and return the response. """
        msg = "issue command %s" % cmd
        logging.debug(msg)
        self._write(cmd)
        return self._read_until("END")

    def close(self) -> str:
        """close telnet connection. """
        return self._write("quit")

    def _write(self, cmd: str) -> typing.Optional[str]:
        if not cmd.endswith("\n"):
            cmd = cmd + "\n"
        resp = self.client.write(cmd.encode("utf8"))
        if resp:
            return resp.decode('utf8')

    def _read_until(self, token: str) -> str:
        return self.client.read_until(token.encode("utf8")).decode("utf8")

    def key_details(self, sort=True, limit=100) -> list:
        """Return a list of tuples containing keys and details. """
        cmd = 'stats cachedump %s %s'
        keys = [key for id in self.slab_ids()
                for key in self._key_regex.findall(self.command(cmd % (id, limit)))]
        if sort:
            return sorted(keys)
        else:
            return keys

    def keys(self, sort=True, limit=100) -> list:
        """Return a list of keys in use. """
        return [key[0] for key in self.key_details(sort=sort, limit=limit)]

    def slab_ids(self) -> typing.List[typing.Any]:
        """Return a list of slab ids in use. """
        return self._slab_regex.findall(self.command('stats items'))

    def stats(self) -> dict:
        """Return a dict containing memcached stats. """
        return dict(self._stat_regex.findall(self.command('stats')))


def wrap_stats(stats) -> dict:
    try:
        del stats['pid']
        del stats['time']

        stats['usage'] = str(100 * float(stats['bytes']) / float(stats['limit_maxbytes']))
        try:
            stats['get_hit_ratio'] = str(
                100 * float(stats['get_hits']) / (float(stats['get_hits']) + float(stats['get_misses'])))
        except ZeroDivisionError:
            stats['get_hit_ratio'] = '0.0'
        try:
            stats['incr_hit_ratio'] = str(
                100 * float(stats['incr_hits']) / (float(stats['incr_hits']) + float(stats['incr_misses'])))
        except ZeroDivisionError:
            stats['incr_hit_ratio'] = '0.0'
        try:
            stats['decr_hit_ratio'] = str(
                100 * float(stats['decr_hits']) / (float(stats['decr_hits']) + float(stats['decr_misses'])))
        except ZeroDivisionError:
            stats['decr_hit_ratio'] = '0.0'
        try:
            stats['delete_hit_ratio'] = str(
                100 * float(stats['delete_hits']) / (float(stats['delete_hits']) + float(stats['delete_misses'])))
        except ZeroDivisionError:
            stats['delete_hit_ratio'] = '0.0'

    except KeyError:
        msg = "access key not exists"
        logging.exception(msg)
    finally:
        return stats


def collect_instances(timeout: float) -> typing.Optional[list]:
    metric = "mc"
    ipaddr = socket.gethostname()
    now = int(time.time())
    step = 60

    data = []
    gauges = [
        'get_hit_ratio',
        'incr_hit_ratio',
        'decr_hit_ratio',
        'delete_hit_ratio',
        'usage',
        'curr_connections',
        'total_connections',
        'bytes',
        'pointer_size',
        'uptime',
        'limit_maxbytes',
        'threads',
        'curr_items',
        'total_items',
        'connection_structures',
    ]

    ports = MemcachedStats.get_instances()
    if not ports:
        msg = "process matched name %s not found" % MemcachedStats.bin_name
        logging.warning(msg)
        return

    msg = "listen port(s) %s found" % ",".join([str(i) for i in ports])
    logging.debug(msg)

    for port in ports:
        port = int(port)
        endpoint = ipaddr
        tags = 'port=%s' % port

        try:
            conn = MemcachedStats(port=port, timeout=timeout)
            stats = wrap_stats(conn.stats())
            conn.close()
        except:
            msg = "query memcached instance stats failed, port=%s" % port
            logging.exception(msg)
            continue

        for key in stats:
            value = float(stats[key])
            if key in gauges:
                suffix = ''
                vtype = 'GAUGE'
            else:
                suffix = '_cps'
                vtype = 'COUNTER'

            i = {
                'metric': '%s.%s%s' % (metric, key, suffix),
                'endpoint': endpoint,
                'timestamp': now,
                'step': step,
                'value': value,
                'counterType': vtype,
                'tags': tags
            }
            data.append(i)

    return data


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sendto",
        help="open-falcon agent PUSH HTTP API URL",
        default="http://127.0.0.1:1988/v1/push",
    )
    parser.add_argument(
        "--timeout",
        help="connection timeout",
        default=2.0,
        type=float,
    )

    args = parser.parse_args()
    result = collect_instances(timeout=args.timeout)
    if result:
        data = json.dumps(result)
        try:
            r = requests.post(
                url=args.sendto,
                data=data,
                timeout=args.timeout)
        except:
            msg = "POST %s failed, req.body.bytes=%d" % (args.sendto, len(data))
            logging.exception(msg)
