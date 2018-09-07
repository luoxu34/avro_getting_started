#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import avro.ipc as ipc
import avro.protocol as protocol


PROTOCOL = protocol.parse(open('mail.avpr').read())

server_addr = ('127.0.0.1', 9090)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('Usage: <to> <from> <body>')
        exit()

    client = ipc.HTTPTransceiver(server_addr[0], server_addr[1])
    requestor = ipc.Requestor(PROTOCOL, client)

    message = dict()
    message['to'] = sys.argv[0]
    message['from'] = sys.argv[1]
    message['body'] = sys.argv[2]

    params = dict(message=message)
    print('Result: ' + requestor.request('send', params))

    client.close()

