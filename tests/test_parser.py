import json
import unittest

import tornado

from http_client import Upstream
from http_client.options import options
from http_client.consul_parser import parse_consul_upstream_config, parse_consul_health_servers_data


class TestParser(unittest.TestCase):

    def test_parse_config(self):
        value = {'Value': json.dumps(
            {
                'hosts': {
                    'default': {
                        'profiles': {
                            'default': {
                                'max_tries': '3',
                                'fail_timeout_sec': '1',
                                'request_timeout_sec': '5',
                                'connect_timeout_sec': '0.2',
                                'max_fails': '5',
                                'max_timeout_tries': '1',
                                'slow_start_interval_sec': '150',
                                'session_required': 'true',
                            }
                        }
                    }
                }
            }
        )}
        config = parse_consul_upstream_config(value)

        self.assertEqual(config['max_tries'], '3')
        self.assertEqual(config['fail_timeout_sec'], '1')
        self.assertEqual(config['request_timeout_sec'], '5')
        self.assertEqual(config['connect_timeout_sec'], '0.2')
        self.assertEqual(config['max_fails'], '5')
        self.assertEqual(config['max_timeout_tries'], '1')
        self.assertEqual(config['slow_start_interval_sec'], '150')
        self.assertEqual(config['session_required'], 'true')

    def test_parse_config_default_value(self):
        value = {'Value': json.dumps(
            {
                'hosts': {
                    'default': {
                        'profiles': {
                            'default': {
                                'max_tries': '3'
                            }
                        }
                    }
                }
            }
        )}
        config = parse_consul_upstream_config(value)

        self.assertEqual(config['max_tries'], '3')
        self.assertEqual(config['fail_timeout_sec'], options.http_client_default_fail_timeout_sec)
        self.assertEqual(config['session_required'], options.http_client_default_session_required)

    def test_parse_health_service(self):
        value = [
            {
                'Node': {
                    'ID': '1',
                    'Node': '',
                    'Address': '1.1.1.1',
                    'Datacenter': 'test',
                },
                'Service': {
                    'ID': '2',
                    'Service': 'app',
                    'Address': '',
                    'Port': 9999,
                    'Weights': {
                        'Passing': 100,
                        'Warning': 0
                    }
                }
            }
        ]

        dc, servers = parse_consul_health_servers_data(value)

        self.assertEqual(len(servers), 1)
        self.assertEqual(servers[0].address, '1.1.1.1:9999')
        self.assertEqual(servers[0].weight, 100)
        self.assertEqual(servers[0].datacenter, 'test')

    def test_parse_health_service_not_test_datacenter(self):
        value = [
            {
                'Node': {
                    'ID': '1',
                    'Node': 'some_name',
                    'Address': '1.1.1.1',
                    'Datacenter': 'test',
                },
                'Service': {
                    'ID': '2',
                    'Service': 'app',
                    'Address': '',
                    'Port': 9999,
                    'Weights': {
                        'Passing': 100,
                        'Warning': 0
                    }
                }
            }
        ]

        dc, servers = parse_consul_health_servers_data(value)

        self.assertEqual(len(servers), 1)

    def test_parse_health_service_not_test_datacenter_with_self_enabled_filter(self):
        tornado.options.options.self_node_filter_enabled = True
        value = [
            {
                'Node': {
                    'ID': '1',
                    'Node': 'some_name',
                    'Address': '1.1.1.1',
                    'Datacenter': 'test',
                },
                'Service': {
                    'ID': '2',
                    'Service': 'app',
                    'Address': '',
                    'Port': 9999,
                    'Weights': {
                        'Passing': 100,
                        'Warning': 0
                    }
                }
            }
        ]

        dc, servers = parse_consul_health_servers_data(value)

        self.assertEqual(len(servers), 0)

    def test_parse_retry_policy(self):
        value = {
            'Value': """{
                "hosts":{
                    "default":{
                        "profiles":{
                            "default":{
                                "max_timeout_tries":"2",
                                "request_timeout_sec":"2",
                                "connect_timeout_sec":"0.1",
                                "max_fails":"5",
                                "retry_policy":{
                                    "503":{
                                        "idempotent":"true"
                                    },
                                    "599":{
                                        "idempotent":"false"
                                    }
                                },
                                "fail_timeout_sec":"2",
                                "max_tries":"3"
                            }
                        }
                    }
                }
            }"""
        }

        config = parse_consul_upstream_config(value)

        upstream = Upstream('some_upstream', config, [])
        self.assertEqual(upstream.retry_policy.statuses, {503: True, 599: False})
