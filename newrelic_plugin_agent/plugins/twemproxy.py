"""
twemproxy (nutcracker)

"""
import logging
import json

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
            if not isinstance(pstats, dict):
                continue
            self.add_pool_stats(pname, pstats)

            # Servers in each pool
            for sname, sstats in pstats.items():
                if not isinstance(sstats, dict):
                    continue
                self.add_server_stats(pname, sname, sstats)

    def add_pool_stats(self, pname, pstats):
        """Add all data points for a pool.

        :param str pname: pool name
        :param dict pstats: pool stats
        """
        # pool stats:
        #   client_eof          "# eof on client connections"
        #   client_err          "# errors on client connections"
        #   client_connections  "# active client connections"
        #   server_ejects       "# times backend server was ejected"
        #   forward_error       "# times we encountered a forwarding error"
        #   fragments           "# fragments created from a multi-vector request"
        self.add_derive_value('Pool/%s/Client EOF' % pname, 'connections', pstats['client_eof'])
        self.add_derive_value('Pool/%s/Client Errors' % pname, 'connections', pstats['client_err'])
        self.add_gauge_value ('Pool/%s/Client Connections' % pname, 'connections', pstats['client_connections'])
        self.add_derive_value('Pool/%s/Server Ejects' % pname, 'ejects', pstats['server_ejects'])
        self.add_derive_value('Pool/%s/Forwarding Errors' % pname, 'errors', pstats['forward_error'])
        self.add_derive_value('Pool/%s/Fragments created' % pname, 'fragments', pstats['fragments'])

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
        self.add_derive_value('Server/%s/Server EOF' % sname, 'connections', sstats['server_eof'])
        self.add_derive_value('Server/%s/Server Errors' % sname, 'connections', sstats['server_err'])
        self.add_derive_value('Server/%s/Server Timeouts' % sname, 'connections', sstats['server_timedout'])
        self.add_gauge_value ('Server/%s/Server Connections' % sname, 'connections', sstats['server_connections'])
        self.add_derive_value('Server/%s/Requests' % sname, 'requests', sstats['requests'])
        self.add_derive_value('Server/%s/Request Bytes' % sname, 'bytes', sstats['request_bytes'])
        self.add_derive_value('Server/%s/Responses' % sname, 'requests', sstats['responses'])
        self.add_derive_value('Server/%s/Response Bytes' % sname, 'bytes', sstats['response_bytes'])
        self.add_derive_value('Server/%s/Queued Incoming Requests' % sname, 'requests', sstats['in_queue'])
        self.add_derive_value('Server/%s/Queued Incoming Bytes' % sname, 'bytes', sstats['in_queue_bytes'])
        self.add_derive_value('Server/%s/Queued Outgoing Requests' % sname, 'requests', sstats['out_queue'])
        self.add_derive_value('Server/%s/Queued Outgoing Bytes' % sname, 'bytes', sstats['out_queue_bytes'])

    def fetch_data(self, connection):
        """Loop in and read in all the data until we have received it all.
        Then parse as JSON.

        :param  socket connection: The connection

        """
        data = super(Twemproxy, self).fetch_data(connection)
        return json.loads(data)
