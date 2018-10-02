#!/usr/bin/env python3

from tornado.ioloop import IOLoop
from tornado.web import FallbackHandler, RequestHandler, Application
from tornado.wsgi import WSGIContainer

from web import app


class MainHandler(RequestHandler):
    def data_received(self, chunk):
        pass

    def get(self):
        self.write("This message comes from Tornado ^_^")


tr = WSGIContainer(app)

application = Application([
    (r"/tornado", MainHandler),
    (r".*", FallbackHandler, dict(fallback=tr)),
])

if __name__ == "__main__":
    application.listen(80)
    IOLoop.instance().start()
