#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='rabbit-alog')

line = sys.stdin.readline()
while line:
    line = line.rstrip('\n')
    channel.basic_publish(exchange='', routing_key='rabbit-alog', body=line)
    print line
    line = sys.stdin.readline()

connection.close()

