"""
twemproxy (nutcracker)

"""
import logging
import json
from collections import defaultdict

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)


class Twemproxy(base.SocketStatsPlugin):
    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 22222
    GUID = 'com.onlulu.newrelic_twemproxy_agent'

    # Pool stats to summarize
    POOL_TOTALS = [
        'client_err',
        'forward_error',
        'server_ejects',
    ]

    def add_datapoints(self, stats):
        """Add all of the data points for each pool.

        :param dict stats: all of the nodes

        """
        # Pools
        for pname, pstats in stats.items():
            if isinstance(pstats, dict):
                self.add_pool_stats(pname, pstats)
                # Servers in each pool
                for sname, sstats in pstats.items():
                    if isinstance(sstats, dict):
                        self.add_server_stats(pname, sname, sstats)

        self.add_totals(stats)

    def add_pool_stats(self, pname, pstats):
        """Add all data points for a pool.

        :param str pname: pool name
        :param dict pstats: pool stats
        """
        # pool stats:
        #   client_connections  "# active client connections"
        #   client_eof          "# eof on client connections"
        #   client_err          "# errors on client connections"
        #   forward_error       "# times we encountered a forwarding error"
        #   fragments           "# fragments created from a multi-vector request"
        #   server_ejects       "# times backend server was ejected"
        self.add_gauge_value ('Pool/%s/Client Connections' % pname, 'connections', pstats['client_connections'])
        self.add_derive_value('Pool/%s/Client EOF' % pname, 'connections', pstats['client_eof'])
        self.add_derive_value('Pool/%s/Client Errors' % pname, 'connections', pstats['client_err'])
        self.add_derive_value('Pool/%s/Forwarding Errors' % pname, 'errors', pstats['forward_error'])
        self.add_derive_value('Pool/%s/Fragments created' % pname, 'fragments', pstats['fragments'])
        self.add_derive_value('Pool/%s/Server Ejects' % pname, 'ejects', pstats['server_ejects'])

    def add_server_stats(self, pname, sname, sstats):
        """Add all data points for a server in a pool.

        :param str pname: pool name
        :param str sname: server name
        :param dict sstats: server stats
        """
        #server stats:
        #  server_eof          "# eof on server connections"
        #  server_err          "# errors on server connections"
        #  server_timedout     "# timeouts on server connections"
        #  server_connections  "# active server connections"
        #  server_ejected_at   "timestamp when server was ejected in usec since epoch"
        #  requests            "# requests"
        #  request_bytes       "total request bytes"
        #  responses           "# respones"
        #  response_bytes      "total response bytes"
        #  in_queue            "# requests in incoming queue"
        #  in_queue_bytes      "current request bytes in incoming queue"
        #  out_queue           "# requests in outgoing queue"
        #  out_queue_bytes     "current request bytes in outgoing queue"
        self.add_derive_value('Server/%s:%s/Server EOF' % (pname, sname), 'connections', sstats['server_eof'])
        self.add_derive_value('Server/%s:%s/Server Errors' % (pname, sname), 'connections', sstats['server_err'])
        self.add_derive_value('Server/%s:%s/Server Timeouts' % (pname, sname), 'connections', sstats['server_timedout'])
        self.add_gauge_value ('Server/%s:%s/Server Connections' % (pname, sname), 'connections', sstats['server_connections'])
        self.add_derive_value('Server/%s:%s/Requests' % (pname, sname), 'requests', sstats['requests'])
        self.add_derive_value('Server/%s:%s/Request Bytes' % (pname, sname), 'bytes', sstats['request_bytes'])
        self.add_derive_value('Server/%s:%s/Responses' % (pname, sname), 'requests', sstats['responses'])
        self.add_derive_value('Server/%s:%s/Response Bytes' % (pname, sname), 'bytes', sstats['response_bytes'])
        self.add_derive_value('Server/%s:%s/Queued Incoming Requests' % (pname, sname), 'requests', sstats['in_queue'])
        self.add_derive_value('Server/%s:%s/Queued Incoming Bytes' % (pname, sname), 'bytes', sstats['in_queue_bytes'])
        self.add_derive_value('Server/%s:%s/Queued Outgoing Requests' % (pname, sname), 'requests', sstats['out_queue'])
        self.add_derive_value('Server/%s:%s/Queued Outgoing Bytes' % (pname, sname), 'bytes', sstats['out_queue_bytes'])

    def add_totals(self, stats):
        """Add totals for some data points (the ones useful for alerts)

        :param dict stats: all

        """
        totals = defaultdict(int)
        for pname, pstats in stats.items():
            if isinstance(pstats, dict):
                for sname in self.POOL_TOTALS:
                    totals[sname] += pstats[sname]

        self.add_derive_value('Totals/Client Errors', 'errors', totals['client_err'])
        self.add_derive_value('Totals/Forwarding Errors', 'errors', totals['forward_error'])
        self.add_derive_value('Totals/Server Ejects', 'errors', totals['server_ejects'])


    def fetch_data(self, connection):
        """Loop in and read in all the data until we have received it all.
        Then parse as JSON.

        :param  socket connection: The connection

        """
        data = super(Twemproxy, self).fetch_data(connection)
        return json.loads(data)
