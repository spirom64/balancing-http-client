Balancing http client for tornado

usage example:

```
@gen.coroutine
def runner():
    http_client_factory = HttpClientFactory('app-name', AsyncHTTPClient(), {
        'backend1': {
            'config': {
                'request_timeout_sec': 0.5
            },
            'servers': [
                {'server': 'http://127.0.0.1:8080'},
                {'server': 'http://127.0.0.2:8080'}
            ],
        }
    })

    http_client = http_client_factory.get_http_client()

    result = yield http_client.get_url('backend1', '/some-url')

    if not result.failed:
        print(result.data)


IOLoop.current().run_sync(runner)
```
