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
        self.add_derive_value('Pool/%s/Hiccup/Client Errors' % pname, 'connections', pstats['client_err'])
        self.add_derive_value('Pool/%s/Hiccup/Forwarding Errors' % pname, 'errors', pstats['forward_error'])
        self.add_derive_value('Pool/%s/Fragments created' % pname, 'fragments', pstats['fragments'])
        self.add_derive_value('Pool/%s/Hiccup/Server Ejects' % pname, 'ejects', pstats['server_ejects'])

    def add_server_stats(self, pname, sname, sstats):
        """Add all data points for a server in a pool.

        :param str pname: pool name
        :param str sname: server name
        :param dict sstats: server stats
        """
        #server stats:
        #  in_queue            "# requests in incoming queue"
        #  in_queue_bytes      "current request bytes in incoming queue"
        #  out_queue           "# requests in outgoing queue"
        #  out_queue_bytes     "current request bytes in outgoing queue"
        #  requests            "# requests"
        #  request_bytes       "total request bytes"
        #  responses           "# respones"
        #  response_bytes      "total response bytes"
        #  server_connections  "# active server connections"
        #  server_ejected_at   "timestamp when server was ejected in usec since epoch"
        #  server_eof          "# eof on server connections"
        #  server_err          "# errors on server connections"
        #  server_timedout     "# timeouts on server connections"
        self.add_derive_value('Server/%s/Queued Incoming Requests' % sname, 'requests', sstats['in_queue'])
        self.add_derive_value('Server/%s/Queued Incoming Bytes' % sname, 'bytes', sstats['in_queue_bytes'])
        self.add_derive_value('Server/%s/Queued Outgoing Requests' % sname, 'requests', sstats['out_queue'])
        self.add_derive_value('Server/%s/Queued Outgoing Bytes' % sname, 'bytes', sstats['out_queue_bytes'])
        self.add_derive_value('Server/%s/Requests' % sname, 'requests', sstats['requests'])
        self.add_derive_value('Server/%s/Request Bytes' % sname, 'bytes', sstats['request_bytes'])
        self.add_derive_value('Server/%s/Responses' % sname, 'requests', sstats['responses'])
        self.add_derive_value('Server/%s/Response Bytes' % sname, 'bytes', sstats['response_bytes'])
        self.add_gauge_value ('Server/%s/Server Connections' % sname, 'connections', sstats['server_connections'])
        self.add_gauge_value ('Server/%s/Server Ejected at' % sname, 'usec', sstats['server_ejected_at'])
        self.add_derive_value('Server/%s/Hiccup/Server EOF' % sname, 'connections', sstats['server_eof'])
        self.add_derive_value('Server/%s/Hiccup/Server Errors' % sname, 'connections', sstats['server_err'])
        self.add_derive_value('Server/%s/Hiccup/Server Timeouts' % sname, 'connections', sstats['server_timedout'])

    def add_totals(self, stats):
        """Add totals for some data points (the ones useful for alerts)

        :param dict stats: all

        """
        # Global sums
        totals = defaultdict(int)
        # Per-pool sums (of server stats)
        ptotals = {}
        for nk, v in stats.items():
            if isinstance(v, dict):  # Pool
                ptotals[nk] = defaultdict(int)
                for pnk, pv in v.items():
                    if isinstance(pv, dict):  # Server
                        for snk, sv in pv.items():
                            ptotals[nk][snk] += sv
                            totals[snk] += sv
                    else:  # Pool stat
                        totals[pnk] += pv

        # Add global sums (for alerts)
        self.add_derive_value('Totals/Client Errors', 'errors', totals['client_err'])
        self.add_derive_value('Totals/Forwarding Errors', 'errors', totals['forward_error'])
        self.add_derive_value('Totals/Server Ejects', 'errors', totals['server_ejects'])
        self.add_derive_value('Totals/Requests', 'requests', totals['requests'])

        # Add per-pool sums
        for pname, pstats in ptotals.items():
            #  defaultdict(<type 'int'>, {u'server_timedout': 0, u'server_err': 0, u'responses': 2629896, u'in_queue_bytes': 0, u'response_bytes': 384540628, u'server_connections': 128, u'request_bytes': 657681621, u'server_ejected_at': 0, u'server_eof': 0, u'out_queue': 0, u'requests': 2629896, u'in_queue': 0, u'out_queue_bytes': 0})
            self.add_derive_value('Pool/%s/Total Queued Incoming Requests' % pname, 'requests', pstats['in_queue'])
            self.add_derive_value('Pool/%s/Total Queued Outgoing Requests' % pname, 'requests', pstats['out_queue'])
            self.add_derive_value('Pool/%s/Total Requests' % pname, 'requests', pstats['requests'])
            self.add_derive_value('Pool/%s/Total Request Bytes' % pname, 'bytes', pstats['request_bytes'])
            self.add_derive_value('Pool/%s/Total Response Bytes' % pname, 'bytes', pstats['response_bytes'])
            self.add_gauge_value ('Pool/%s/Total Server Connections' % pname, 'connections', pstats['server_connections'])
            self.add_derive_value('Pool/%s/Hiccup/Total Server EOF' % pname, 'connections', pstats['server_eof'])
            self.add_derive_value('Pool/%s/Hiccup/Total Server Errors' % pname, 'connections', pstats['server_err'])
            self.add_derive_value('Pool/%s/Hiccup/Total Server Timeouts' % pname, 'connections', pstats['server_timedout'])

    def fetch_data(self, connection):
        """Loop in and read in all the data until we have received it all.
        Then parse as JSON.

        :param  socket connection: The connection

        """
        data = super(Twemproxy, self).fetch_data(connection)
        return json.loads(data)
