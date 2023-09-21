#!/usr/bin/env python3

import yaml
import re
import paho.mqtt.client as mqtt
from prometheus_client import Gauge, start_http_server
import logging
import sys


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
    def __init__(self, *, subscribe, labels={}, value_map={}):
        assert '#' not in subscribe or subscribe.index('#') == len(subscribe)-1

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
        self.labels = label_mapping
        self.value_map = value_map

        r = '^' + topic.replace('+', '[^\/]+')
        if topic[-1] not in ['+', '#']:
            r += '$'
        else:
            r = r.rstrip('#')
        self._re_match_topic = re.compile(r)

        self._metrics = {}
        self._enum_prev_values = set()

    def match_topic(self, topic):
        return self._re_match_topic.match(topic)

    def precedence(self):
        # Specialcase '#' paths to be lower.
        return len(self.topic.split('/')) + (0 if self.topic[-1] == '#' else 0.5)

    def interpret(self, topic, payload):
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

        if value_mode == 'number':
            try:
                remap = self.value_map.get(payload.strip())
                if remap is not None:
                    set_value = float(remap)
                else:
                    set_value = float(payload.split(' ')[0].strip())
            except:
                logging.debug('invalid value: %s -> "%s"', topic, payload)
                return None
        elif value_mode == 'enum':
            set_value = 1
            for v in self._enum_prev_values:
                enum_label = next(label for (label, i) in self.labels if i == 'enum')
                prom_metric.labels(**{**label_values, enum_label: v}).set(0)
            self._enum_prev_values.add(payload)

        return Metric(metric, label_values, set_value)

    def ingest(self, topic, payload):
        m = self.interpret(topic, payload)
        if not m:
            return

        prom_metric = self._metrics.get(m.metric_name)
        if not prom_metric:
            labels = list(m.label_values.keys())
            prom_metric = Gauge(m.metric_name, '', labels)
            self._metrics[m.metric_name] = prom_metric

        logging.debug('%s %s %s', m.metric_name, m.label_values, m.value)
        if m.label_values:
            prom_metric = prom_metric.labels(**m.label_values)
        prom_metric.set(m.value)


#
# Unit Tests
#
m1 = Mapping(subscribe='sensors/#')
assert m1.topic == 'sensors/#'
assert not m1.match_topic('sensors')
assert m1.match_topic('sensors/foo')
assert m1.interpret('sensors/foo', '12') == Metric('sensors_foo', {}, 12.0)

m2 = Mapping(subscribe='sensors/+location/#')
assert m2.topic == 'sensors/+/#'
assert m2.match_topic('sensors/foo/temperature')
assert m2.interpret('sensors/foo/temperature', '12') \
    == Metric('sensors_temperature', {'location': 'foo'}, 12.0)

m3 = Mapping(subscribe='sensors/+location/version', labels={'version': 'enum'})
assert m3.topic == 'sensors/+/version'
assert m3.match_topic('sensors/foo/version')
assert not m3.match_topic('sensors/foo/version/bar')
assert m3.interpret('sensors/foo/version', 'asdf') \
    == Metric('sensors_version', {'location': 'foo', 'version': 'asdf'}, 1.0)

m4 = Mapping(subscribe='bitlair/state', value_map={'open': 1.0, 'closed': 0.0})
assert m4.interpret('bitlair/state', 'open') == Metric('bitlair_state', {}, 1.0)
assert m4.interpret('bitlair/state', 'closed') == Metric('bitlair_state', {}, 0.0)


with open(sys.argv[1]) as file:
    config = yaml.load(file, Loader=yaml.Loader)

mqtt_host = config['mqtt']['host']
mqtt_port = config['mqtt']['port']

mappings = [Mapping(**export) for export in config['export']]
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
