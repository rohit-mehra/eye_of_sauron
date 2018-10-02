#!/usr/bin/env python3

from web import app


app.run(host='0.0.0.0', debug=False, threaded=True, port=3333)
