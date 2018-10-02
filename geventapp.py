#!/usr/bin/env python3

import gevent.monkey

gevent.monkey.patch_all()

from gevent.pywsgi import WSGIServer
from web import app

server = WSGIServer(('0.0.0.0', 5000), app)
server.serve_forever()
