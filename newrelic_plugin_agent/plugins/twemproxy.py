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
        """Add all of the data points for each pool

        :param dict stats: all of the nodes

        """
        #from pprint import pprint; pprint(stats)
        # Per pool stats
        for pname, pstats in stats.items():
            # Filter out global stuff
            if not isinstance(pstats, dict):
                continue
            # pool stats:
            #   client_eof          "# eof on client connections"
            #   client_err          "# errors on client connections"
            #   client_connections  "# active client connections"
            #   server_ejects       "# times backend server was ejected"
            #   forward_error       "# times we encountered a forwarding error"
            #   fragments           "# fragments created from a multi-vector request"
            self.add_derive_value('Client EOF', 'connections', pstats['client_eof'])
            self.add_derive_value('Client Errors', 'connections', pstats['client_err'])
            self.add_gauge_value('Client Connections', 'connections', pstats['client_connections'])
            self.add_derive_value('Server Ejects', 'ejects', pstats['server_ejects'])
            self.add_derive_value('Forwarding Errors', 'errors', pstats['forward_error'])
            self.add_derive_value('Fragments created', 'fragments', pstats['fragments'])

            # Per server stats
            if isinstance(pstats, dict):
                for sname, sstats in pstats.items():
                    # TODO: per server stats!
                    pass




    def fetch_data(self, connection):
        """Loop in and read in all the data until we have received it all.
        Then parse as JSON.

        :param  socket connection: The connection

        """
        data = super(Twemproxy, self).fetch_data(connection)
        return json.loads(data)
