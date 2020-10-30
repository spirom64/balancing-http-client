from http import HTTPStatus

from tornado.testing import gen_test, bind_unused_port

from tests.test_balancing_base import BalancingClientMixin, WorkingServerTestCase


class NotExistingServerTest(BalancingClientMixin, WorkingServerTestCase):

    def setUp(self):
        self.not_serving_socket, not_serving_port = bind_unused_port()
        self.not_serving_socket.close()
        super().setUp()
        self.register_ports_for_upstream(not_serving_port, self.get_http_port())

    def tearDown(self):
        super().tearDown()

    @gen_test
    async def test_non_existing_idempotent_retries(self):
        response = await self.balancing_client.get_url('test', '/')
        self.assertEqual(HTTPStatus.OK, response.response.code)

    @gen_test
    async def test_non_existing_non_idempotent_retries(self):
        response = await self.balancing_client.post_url('test', '/')
        self.assertEqual(HTTPStatus.OK, response.response.code)
