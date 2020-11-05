from tornado.httpclient import AsyncHTTPClient
from tornado.testing import AsyncHTTPTestCase
from tornado.web import RequestHandler, Application

from http_client import HttpClientFactory, Server


class WorkingHandler(RequestHandler):
    def get(self):
        self.set_status(200)

    def post(self):
        self.set_status(200)


class WorkingServerTestCase(AsyncHTTPTestCase):

    def get_app(self):
        return Application([
            (r"/", WorkingHandler),
        ])


class BalancingClientMixin:

    def setUp(self):
        AsyncHTTPClient.configure('tornado.curl_httpclient.CurlAsyncHTTPClient')
        super().setUp()
        self.http_client_factory = HttpClientFactory('testapp', self.http_client, {})
        self.balancing_client = self.http_client_factory.get_http_client()

    def get_upstream_config(self):
        return {'max_fails': 10, 'fail_timeout_sec': 0.1, 'request_timeout_sec': 0.5}

    def register_ports_for_upstream(self, *ports):
        self.http_client_factory.register_upstream('test', self.get_upstream_config(),
                                                   [Server(f'127.0.0.1:{port}') for port in ports])
