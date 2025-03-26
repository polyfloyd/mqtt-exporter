#!/usr/bin/env python3

import yaml
import re
import paho.mqtt.client as mqtt
from prometheus_client import Counter, Info, Gauge, Enum, start_http_server
import logging
import sys
import math
import subprocess


def jq(raw_json, path):
    o = subprocess.run(['jq', path], input=raw_json, capture_output=True)
    return o.stdout.strip().decode('utf-8')


class Metric:
    def __init__(self, metric_type, metric_name, label_values, value):
        self.metric_type = metric_type
        self.metric_name = metric_name
        self.label_values = label_values
        self.value = value

    def __str__(self):
        return '%s %s %s' % (self.metric_name, self.label_values, self.value)

    def __eq__(self, other):
        return self.metric_name == other.metric_name \
            and self.label_values == other.label_values \
            and self.value == other.value


class Mapping:
    def __init__(self, *, subscribe, metric_name=None, metric_type='gauge', labels={},
                 value_regex=None, value_json=None, enum_states=[], info_name='value'):
        assert '#' not in subscribe or subscribe.index('#') == len(subscribe)-1
        assert metric_type in {'counter', 'enum', 'info', 'gauge', None}
        assert not ('payload' in labels.values() and metric_type in ['gauge', 'info', 'enum'])
        assert (metric_type == 'enum') == (len(enum_states) > 0)
        assert metric_type != 'info' or info_name
        assert set(labels.values()) - {'payload'} == set()
        assert value_regex is None or value_json is None

        re_topic_labels = re.compile(r'\+(\w+)')

        s = [s.count('/') for s in subscribe.split('+')][:-1]
        label_indices = [c+sum(s[:i]) for (i, c) in enumerate(s)]
        label_names = re_topic_labels.findall(subscribe)
        assert len(label_indices) == len(label_names)
        label_mapping = list(zip(label_names, label_indices))

        for (k, v) in labels.items():
            label_mapping.append((k,v))

        topic = re_topic_labels.sub('+', subscribe)

        self.topic = topic
        self.type = metric_type
        self.labels = label_mapping
        self.enum_states = enum_states
        self.info_name = info_name

        r = '^' + topic.replace('+', r'[^\/]+')
        if topic[-1] not in ['+', '#']:
            r += '$'
        else:
            r = r.rstrip('#')
        self._re_match_topic = re.compile(r)

        if value_regex:
            r = re.compile(value_regex)
            self._value_extract = lambda s: r.match(s)[1]
        elif value_json:
            self._value_extract = lambda s: jq(s.encode('utf-8'), value_json)
        else:
            self._value_extract = lambda s: s

        self._metric_name = metric_name
        self._metrics = {}

    def match_topic(self, topic):
        return self._re_match_topic.match(topic)

    def precedence(self):
        # Specialcase '#' paths to be lower.
        return len(self.topic.split('/')) + (0 if self.topic[-1] == '#' else 0.5)

    def interpret(self, topic, payload):
        if self.type is None:
            return None

        parts = topic.split('/')

        payload = self._value_extract(payload)

        value_mode = 'number'
        label_values = {}
        label_indices = set()
        if self.type in ['enum', 'info']:
            value_mode = 'string'
        is_payload_label = False
        for (label, i) in self.labels:
            if i == 'payload':
                is_payload_label = True
                label_values[label] = payload
                value_mode = 'string'
                continue
            label_indices.add(i)
            label_values[label] = parts[i]

        metric_name = self._metric_name
        if not metric_name:
            metric_name = '_'.join(p for (i, p) in enumerate(parts) if i not in label_indices) \
                .replace('-', '_').lower()

        set_value = payload
        if is_payload_label:
            set_value = 1.0
        elif value_mode == 'number':
            try:
                set_value = float(payload.split(' ')[0].strip())
            except Exception:
                logging.debug('invalid value: %s -> "%s"', topic, payload)
                return None
        elif value_mode == 'string':
            set_value = payload

        return Metric(self.type, metric_name, label_values, set_value)

    def ingest(self, topic, payload):
        m = self.interpret(topic, payload)
        if not m:
            return

        prom_metric = self._metrics.get(m.metric_name)
        if not prom_metric:
            labels = list(m.label_values.keys())
            if self.type == 'counter':
                prom_metric = Counter(m.metric_name, '', labels)
            elif self.type == 'info':
                prom_metric = Info(m.metric_name, '', labels)
            elif self.type == 'enum':
                prom_metric = Enum(m.metric_name, '', labels, states=self.enum_states)
            elif self.type == 'gauge':
                prom_metric = Gauge(m.metric_name, '', labels)
            self._metrics[m.metric_name] = prom_metric

        logging.debug('%s %s %s', m.metric_name, m.label_values, m.value)
        if m.label_values:
            prom_metric = prom_metric.labels(**m.label_values)

        if self.type == 'counter':
            prom_metric.inc(m.value)
        elif self.type == 'info':
            prom_metric.info({self.info_name: m.value})
        elif self.type == 'enum':
            prom_metric.state(m.value)
        elif self.type == 'gauge':
            prom_metric.set(m.value)


class Router:
    def __init__(self, mappings):
        # Sort to have longer paths take precedence.
        mappings.sort(key=lambda m: m.precedence(), reverse=True)
        self.mappings = mappings

    def route(self, topic):
        mappings = []

        prev_precedence = -math.inf
        for mapping in self.mappings:
            if mapping.match_topic(topic):
                precedence = mapping.precedence()
                if prev_precedence > precedence:
                    break
                mappings.append(mapping)
                prev_precedence = precedence

        return mappings


def main():
    with open(sys.argv[1]) as file:
        config = yaml.load(file, Loader=yaml.Loader)

    logging.basicConfig(level=config.get('log_level', 'INFO'))

    mqtt_host = config['mqtt']['host']
    mqtt_port = config['mqtt']['port']

    mappings = [Mapping(**export) for export in config['export']]
    router = Router(mappings)

    def on_connect(client, userdata, flags, rc):
        logging.info('mqtt connected')
        for topic in {m.topic for m in mappings}:
            logging.info('subscribing to %s' % topic)
            client.subscribe(topic)

    def on_message(client, userdata, msg):
        try:
            payload = msg.payload.decode()
        except Exception:
            logging.debug('non utf-8 message: %s -> "%s"', msg.topic, msg.payload)
            return
        routed_mappings = router.route(msg.topic)
        for m in routed_mappings:
            m.ingest(msg.topic, payload)
        if len(routed_mappings) == 0:
            logging.debug('unmatched topic: %s', msg.topic)

    prometheus_port = config['prometheus']['port']
    start_http_server(prometheus_port)
    logging.info('started prometheus exporter on port %d', prometheus_port)

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(mqtt_host, mqtt_port, 60)
    client.loop_forever()

if __name__ == '__main__':
    main()
