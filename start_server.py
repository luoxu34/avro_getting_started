#!/usr/bin/env python
# -*- coding:utf-8 -*-

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import avro.ipc as ipc
import avro.protocol as protocol
import avro.schema as schema


PROTOCOL = protocol.parse(open('mail.avpr').read())


class MailResponder(ipc.Responder):
    def __init__(self):
        ipc.Responder.__init__(self, PROTOCOL)

    def invoke(self, msg, req):
        if msg.name == 'send':
            message = req['message']
            return ('Sent message to {to} from {from} with body {body}'.format(**message))
        else:
            raise schema.AvroException('unexpected message:', msg.getname())


class MailHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.responder = MailResponder()
        call_request_reader = ipc.FramedReader(self.rfile)
        call_request = call_request_reader.read_framed_message()
        resp_body = self.responder.respond(call_request)

        self.send_response(200)
        self.send_header('Content-Type', 'avro/binary')
        self.end_headers()

        resp_write = ipc.FramedWriter(self.wfile)
        resp_write.write_framed_message(resp_body)


server_addr = ('127.0.0.1', 9090)


if __name__ == '__main__':
    server = HTTPServer(server_addr, MailHandler)
    server.allow_reuse_address = True
    server.serve_forever()

