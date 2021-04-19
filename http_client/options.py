from tornado.options import define, options as tornado_options


options = tornado_options

define('datacenter', default=None, type=str)
define('datacenters', default=[], type=list)

define('timeout_multiplier', default=1.0, type=float)
define('http_client_default_connect_timeout_sec', default=0.2, type=float)
define('http_client_default_request_timeout_sec', default=2.0, type=float)
define('http_client_default_max_tries', default=2, type=int)
define('http_client_default_max_timeout_tries', default=1, type=int)
define('http_client_default_max_fails', default=0, type=int)
define('http_client_default_fail_timeout_sec', default=10, type=float)
define('http_client_default_retry_policy', default={599: False, 503: False}, type=dict)
define('http_client_default_retry_policy_cassandra', default='timeout,http_503', type=str)
define('http_proxy_host', default=None, type=str)
define('http_proxy_port', default=3128, type=int)
define('http_client_allow_cross_datacenter_requests', default=False, type=bool)
define('self_node_filter_enabled', default=False, type=bool)
if 'node_name' not in options:
    define('node_name', default='', type=str)
