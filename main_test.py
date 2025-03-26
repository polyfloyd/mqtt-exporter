from main import Mapping, Metric, Router


def test_wildcard_suffix():
    m1 = Mapping(subscribe='sensors/#')
    assert m1.topic == 'sensors/#'
    assert not m1.match_topic('sensors')
    assert m1.match_topic('sensors/foo')
    assert m1.interpret('sensors/foo', '12') == Metric('gauge', 'sensors_foo', {}, 12.0)

def test_label_wildcard():
    m2 = Mapping(subscribe='sensors/+location/#')
    assert m2.topic == 'sensors/+/#'
    assert m2.match_topic('sensors/foo/temperature')
    assert m2.interpret('sensors/foo/temperature', '12') \
        == Metric('gauge', 'sensors_temperature', {'location': 'foo'}, 12.0)

def test_info_type():
    m3 = Mapping(subscribe='sensors/+location/version', metric_type='info')
    assert m3.topic == 'sensors/+/version'
    assert m3.match_topic('sensors/foo/version')
    assert not m3.match_topic('sensors/foo/version/bar')
    assert m3.interpret('sensors/foo/version', 'asdf') == Metric('info', 'sensors_version', {'location': 'foo'}, 'asdf')
    assert m3.interpret('sensors/foo/version', 'qwer') == Metric('info', 'sensors_version', {'location': 'foo'}, 'qwer')

def test_enum_type():
    m4 = Mapping(subscribe='bitlair/state', metric_type='enum', enum_states=['open', 'closed'])
    assert m4.interpret('bitlair/state', 'open') == Metric('enum', 'bitlair_state', {}, 'open')
    assert m4.interpret('bitlair/state', 'closed') == Metric('enum', 'bitlair_state', {}, 'closed')

def test_counter_type():
    m5 = Mapping(subscribe='bitlair/pos/product', metric_type='counter', labels={'product': 'payload'})
    assert m5.interpret('bitlair/pos/product', 'Tosti') == Metric('counter', 'bitlair_pos_product', {'product': 'Tosti'}, 1.0)
    assert m5.interpret('bitlair/pos/product', 'Tosti') == Metric('counter', 'bitlair_pos_product', {'product': 'Tosti'}, 1.0)

def test_regex_value():
    m6 = Mapping(subscribe='bitlair/snmp/tx', value_regex='^.+:(.+):.+')
    assert m6.interpret('bitlair/snmp/tx', '1695557017:720167:29751') == Metric('gauge', 'bitlair_snmp_tx', {}, 720167.0)

def test_json_value():
    m7 = Mapping(subscribe='bitlair/power/shelly', value_json='.apower')
    assert m7.interpret('bitlair/power/shelly', '{"apower": 1337.0}') == Metric('gauge', 'bitlair_power_shelly', {}, 1337.0)

def test_router():
    m1 = Mapping(subscribe='sensors/#')
    m3 = Mapping(subscribe='sensors/+location/version', metric_type='info')
    r1 = Router([m1, m3])
    assert [m.topic for m in r1.route('sensors/bar')] == ['sensors/#']
    assert [m.topic for m in r1.route('sensors/foo/version')] == ['sensors/+/version']
