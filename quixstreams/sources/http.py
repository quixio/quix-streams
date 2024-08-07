import json
import logging

from http.server import HTTPServer, BaseHTTPRequestHandler


from .base import BaseSource

logger = logging.getLogger(__name__)


class HTTPSource(BaseSource):
    def __init__(
        self,
        listen_address: str = "127.0.0.1",
        port: int = 8000,
        shutdown_timeout: int = 10,
    ) -> None:
        super().__init__(shutdown_timeout)

        self._port = port
        self._listen_address = listen_address

        Handler.produce = self.produce
        self._server = HTTPServer((self._listen_address, self._port), Handler)

    def run(self):
        try:
            self._server.serve_forever()
        except BaseException:
            logger.exception("fooo")
            raise


class Handler(BaseHTTPRequestHandler):

    def do_POST(self):
        self.send_response(200)

        data = self.rfile.read(int(self.headers.get("Content-Length", -1)))
        self.produce(value=data)

        self.send_header("content-type", "application/json")
        self.end_headers()
        data = json.dumps({"status": "ok"})
        self.wfile.write(data.encode())
