#!/usr/bin/env python3

import yaml
import re
import paho.mqtt.client as mqtt
from prometheus_client import Gauge, start_http_server
import logging
import sys


class Mapping:
    def __init__(self, topic, labels):
        self.topic = topic
        self.labels = labels
        self._re_match_topic = re.compile('^'+topic.replace('+', '[^\/]+').rstrip('#'))
        self._metrics = {}
        self._enum_prev_values = set()

    def match_topic(self, topic):
        return self._re_match_topic.match(topic)

    def precedence(self):
        # Specialcase '#' paths to be lower.
        return len(self.topic.split('/')) + (0 if self.topic[-1] == '#' else 0.5)

    def ingest(self, topic, payload):
        parts = topic.split('/')

        value_mode = 'number'
        label_values = {}
        label_indices = set()
        for (label, i) in self.labels:
            if i == 'enum':
                value_mode = 'enum'
                label_values[label] = payload
                break
            label_indices.add(i)
            label_values[label] = parts[i]

        metric = '_'.join(p for (i, p) in enumerate(parts) if i not in label_indices)
        metric = metric.replace('-', '_').lower()
        prom_metric = self._metrics.get(metric)
        if not prom_metric:
            labels = list(label_values.keys())
            prom_metric = Gauge(metric, '', labels)
            self._metrics[metric] = prom_metric

        if value_mode == 'number':
            try:
                set_value = float(payload.split(' ')[0].strip())
            except:
                logging.debug('invalid value: %s -> "%s"', topic, payload)
                return
        elif value_mode == 'enum':
            set_value = 1
            for v in self._enum_prev_values:
                enum_label = next(label for (label, i) in self.labels if i == 'enum')
                prom_metric.labels(**{**label_values, enum_label: v}).set(0)
            self._enum_prev_values.add(payload)

        logging.debug('%s %s %s', metric, label_values, set_value)
        if label_values:
            prom_metric = prom_metric.labels(**label_values)
        prom_metric.set(set_value)


with open(sys.argv[1]) as file:
    config = yaml.load(file, Loader=yaml.Loader)

mqtt_host = config['mqtt']['host']
mqtt_port = config['mqtt']['port']

mappings = []
re_topic_labels = re.compile(r'\+(\w+)')
for export in config['export']:
    subscribe = export['subscribe']
    assert '#' not in subscribe or subscribe.index('#') == len(subscribe)-1

    s = [s.count('/') for s in subscribe.split('+')][:-1]
    label_indices = [c+sum(s[:i]) for (i, c) in enumerate(s)]
    label_names = re_topic_labels.findall(subscribe)
    assert len(label_indices) == len(label_names)
    label_mapping = list(zip(label_names, label_indices))

    for (k, v) in export.get('labels', {}).items():
        label_mapping.append((k,v))

    topic = re_topic_labels.sub('+', subscribe)
    mappings.append(Mapping(topic, label_mapping))

# Sort to have longer paths take precedence.
mappings.sort(key=lambda m: m.precedence(), reverse=True)


def on_connect(client, userdata, flags, rc):
    logging.info('mqtt connected')
    for m in mappings:
        logging.info('subscribing to %s' % m.topic)
        client.subscribe(m.topic)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
    except:
        logging.debug('non utf-8 message: %s -> "%s"', msg.topic, msg.payload)
        return
    for mapping in mappings:
        if mapping.match_topic(msg.topic):
            mapping.ingest(msg.topic, payload)
            break
        else:
            logging.debug('unmatched topic: %s', msg.topic)

def main():
    logging.basicConfig(level=logging.INFO)

    prometheus_port = config['prometheus']['port']
    start_http_server(prometheus_port)
    logging.info('started prometheus exporter on port %d', prometheus_port)

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(mqtt_host, mqtt_port, 60)
    while True:
        client.loop()

if __name__ == '__main__':
    main()
