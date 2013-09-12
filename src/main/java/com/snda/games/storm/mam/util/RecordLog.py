#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import pika

connection = pika.BlockingConnection(pika.URLParameters('amqp://test:test@localhost:5672/testlogs'))
#connection = pika.BlockingConnection(pika.ConnectionParameters(virtual_host='testlogs',host='localhost'))
channel = connection.channel()

exchange = "recordlogs"
channel.exchange_declare(exchange, type='direct')
routinKey = "record"

line = sys.stdin.readline()
while line:
    line = line.rstrip('\n')
    channel.basic_publish(exchange, routing_key=routinKey, body=line)
    print line
    line = sys.stdin.readline()

connection.close()
