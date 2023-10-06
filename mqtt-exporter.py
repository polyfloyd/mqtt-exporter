#!/usr/bin/env python3

import yaml
import re
import paho.mqtt.client as mqtt
from prometheus_client import Counter, Gauge, start_http_server
import logging
import sys
import math


class Metric:
    def __init__(self, metric_name, label_values, value):
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
    def __init__(self, *, subscribe, metric_name=None, metric_type='gauge', labels={}, value_map={},
                 value_regex='^(.*)$'):
        assert '#' not in subscribe or subscribe.index('#') == len(subscribe)-1
        assert metric_type in {'counter', 'gauge', None}

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
        self.value_map = value_map

        r = '^' + topic.replace('+', '[^\/]+')
        if topic[-1] not in ['+', '#']:
            r += '$'
        else:
            r = r.rstrip('#')
        self._re_match_topic = re.compile(r)

        self._value_regex = re.compile(value_regex)
        self._metric_name = metric_name
        self._metrics = {}
        self._enum_prev_values = set()

    def match_topic(self, topic):
        return self._re_match_topic.match(topic)

    def precedence(self):
        # Specialcase '#' paths to be lower.
        return len(self.topic.split('/')) + (0 if self.topic[-1] == '#' else 0.5)

    def interpret(self, topic, payload):
        if self.type is None:
            return []

        parts = topic.split('/')

        payload = self._value_regex.match(payload)[1]

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

        metric_name = self._metric_name
        if not metric_name:
            metric_name = '_'.join(p for (i, p) in enumerate(parts) if i not in label_indices) \
                .replace('-', '_').lower()

        metrics = []
        if value_mode == 'number':
            try:
                remap = self.value_map.get(payload.strip())
                if remap is not None:
                    set_value = float(remap)
                else:
                    set_value = float(payload.split(' ')[0].strip())
            except:
                logging.debug('invalid value: %s -> "%s"', topic, payload)
                return []
        elif value_mode == 'enum':
            set_value = 1.0
            if self.type == 'gauge':
                for v in self._enum_prev_values:
                    enum_label = next(label for (label, i) in self.labels if i == 'enum')
                    metrics.append(Metric(metric_name, {**label_values, enum_label: v}, 0.0))
                self._enum_prev_values.add(payload)

        metrics.append(Metric(metric_name, label_values, set_value))
        return metrics

    def ingest(self, topic, payload):
        for m in self.interpret(topic, payload):
            prom_metric = self._metrics.get(m.metric_name)
            if not prom_metric:
                labels = list(m.label_values.keys())
                if self.type == 'counter':
                    prom_metric = Counter(m.metric_name, '', labels)
                else:
                    prom_metric = Gauge(m.metric_name, '', labels)
                self._metrics[m.metric_name] = prom_metric

            logging.debug('%s %s %s', m.metric_name, m.label_values, m.value)
            if m.label_values:
                prom_metric = prom_metric.labels(**m.label_values)

            if self.type == 'counter':
                prom_metric.inc(m.value)
            else:
                prom_metric.set(m.value)


class Router:
    def __init__(self, mappings):
        # Sort to have longer paths take precedence.
        mappings.sort(key=lambda m: m.precedence(), reverse=True)
        self.mappings = mappings

    def route(self, topic):
        mappings = []

        prev_precedence = -math.inf
        matched = False
        for mapping in self.mappings:
            if mapping.match_topic(topic):
                precedence = mapping.precedence()
                if prev_precedence > precedence:
                    break
                matched = True
                mappings.append(mapping)
                prev_precedence = precedence

        return mappings


#
# Unit Tests
#
m1 = Mapping(subscribe='sensors/#')
assert m1.topic == 'sensors/#'
assert not m1.match_topic('sensors')
assert m1.match_topic('sensors/foo')
assert m1.interpret('sensors/foo', '12') == [Metric('sensors_foo', {}, 12.0)]

m2 = Mapping(subscribe='sensors/+location/#')
assert m2.topic == 'sensors/+/#'
assert m2.match_topic('sensors/foo/temperature')
assert m2.interpret('sensors/foo/temperature', '12') \
    == [Metric('sensors_temperature', {'location': 'foo'}, 12.0)]

m3 = Mapping(subscribe='sensors/+location/version', labels={'version': 'enum'})
assert m3.topic == 'sensors/+/version'
assert m3.match_topic('sensors/foo/version')
assert not m3.match_topic('sensors/foo/version/bar')
assert m3.interpret('sensors/foo/version', 'asdf') \
    == [Metric('sensors_version', {'location': 'foo', 'version': 'asdf'}, 1.0)]
assert m3.interpret('sensors/foo/version', 'qwer') \
    == [Metric('sensors_version', {'location': 'foo', 'version': 'asdf'}, 0.0),
        Metric('sensors_version', {'location': 'foo', 'version': 'qwer'}, 1.0)]

m4 = Mapping(subscribe='bitlair/state', value_map={'open': 1.0, 'closed': 0.0})
assert m4.interpret('bitlair/state', 'open') == [Metric('bitlair_state', {}, 1.0)]
assert m4.interpret('bitlair/state', 'closed') == [Metric('bitlair_state', {}, 0.0)]

m5 = Mapping(subscribe='bitlair/pos/product', metric_type='counter', labels={'product': 'enum'})
assert m5.interpret('bitlair/pos/product', 'Tosti') == [Metric('bitlair_pos_product', {'product': 'Tosti'}, 1.0)]
assert m5.interpret('bitlair/pos/product', 'Tosti') == [Metric('bitlair_pos_product', {'product': 'Tosti'}, 1.0)]

m6 = Mapping(subscribe='bitlair/snmp/tx', value_regex='^.+:(.+):.+')
m6.interpret('bitlair/snmp/tx', '1695557017:720167:29751') \
    == [Metric('bitlair_snmp_tx', {}, 720167.0)]

topics = lambda mm: [m.topic for m in mm]
r1 = Router([m1, m3])
assert topics(r1.route('sensors/bar')) == ['sensors/#']
assert topics(r1.route('sensors/foo/version')) == ['sensors/+/version']


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
        for m in mappings:
            logging.info('subscribing to %s' % m.topic)
            client.subscribe(m.topic)

    def on_message(client, userdata, msg):
        try:
            payload = msg.payload.decode()
        except:
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
    while True:
        client.loop()

if __name__ == '__main__':
    main()
