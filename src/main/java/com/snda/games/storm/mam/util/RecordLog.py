#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='logs', type='direct')
routinKey = "recordlog"

line = sys.stdin.readline()
while line:
    line = line.rstrip('\n')
    channel.basic_publish(exchange='logs', routing_key=routinKey, body=line)
    print line
    line = sys.stdin.readline()

connection.close()