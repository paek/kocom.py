#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
 python kocom script

 : forked from script written by kyet, 룰루해피, 따분, Susu Daddy

 apt-get install mosquitto
 python3 -m pip install pyserial
 python3 -m pip install paho-mqtt
'''
import sys
import time
import platform
import threading
import queue
import random
import json
import logging
import configparser
import socket
import serial
import paho.mqtt.client as mqtt

# define -------------------------------
SW_VERSION = '2022.04.10'
CONFIG_FILE = 'kocom.conf'
BUF_SIZE = 100

READ_WRITE_GAP = 0.03     # minimal time interval between last read to write
POLLING_INTERVAL = 300    # polling interval

HEADER_H = 'aa55'
TRAILER_H = '0d0d'
PACKET_SIZE = 21          # total size in byte
CHKSUM_POSITION = 18      # -th byte

type_t_dic = {'30b': 'send', '30d': 'ack'}
seq_t_dic = {'c': 1, 'd': 2, 'e': 3, 'f': 4}
device_t_dic = {
    '01': 'wallpad',
    '0e': 'light',
    '2c': 'gas',
    '36': 'thermo',
    '3b': 'plug',
    '44': 'elevator',
    '48': 'fan'
}
cmd_t_dic = {
    '00': 'state',
    '01': 'on',
    '02': 'off',
    '3a': 'query'
}
room_t_dic = {
    '00': 'livingroom',
    '01': 'bedroom',
    '02': 'room1',
    '03': 'room2'
}

type_h_dic = {v: k for k, v in type_t_dic.items()}
seq_h_dic = {v: k for k, v in seq_t_dic.items()}
device_h_dic = {v: k for k, v in device_t_dic.items()}
cmd_h_dic = {v: k for k, v in cmd_t_dic.items()}
room_h_dic = {
    'livingroom': '00',
    'myhome': '00',
    'bedroom': '01',
    'room1': '02',
    'room2': '03'
}

# serial/socket communication class & functions--------------------

class RS485Wrapper:
    def __init__(self, serial_port=None, socket_server=None, socket_port=0):
        if socket_server is None:
            self.type = 'serial'
            self.serial_port = serial_port
        else:
            self.type = 'socket'
            self.socket_server = socket_server
            self.socket_port = socket_port
        self.last_read_time = 0
        self.conn = False

    def connect(self):
        self.close()
        self.last_read_time = 0
        if self.type == 'serial':
            self.conn = self.__connect_serial(self.serial_port)
        elif self.type == 'socket':
            self.conn = self.__connect_socket(self.socket_server, self.socket_port)
        return self.conn

    def __connect_serial(self, serial_port):
        if serial_port is None:
            os_platfrom = platform.system()
            if os_platfrom == 'Linux':
                serial_port = '/dev/ttyUSB0'
            else:
                serial_port = 'com3'
        try:
            ser = serial.Serial(serial_port, 9600, timeout=1)
            ser.bytesize = 8
            ser.stopbits = 1
            if ser.is_open is False:
                raise Exception('Not ready')
            logging.info("[RS485] Serial connected : %s", ser)
            return ser
        except Exception as exc:
            logging.error("[RS485] Serial open failure : %s", exc)
            return False

    def __connect_socket(self, socket_server, socket_port):
        sock = socket.socket()
        sock.settimeout(10)
        try:
            sock.connect((socket_server, socket_port))
        except Exception as exc:
            logging.error("[RS485] Socket connection failure : %s | server %s, port %d",
                exc, socket_server, socket_port)
            return False
        logging.info("[RS485] Socket connected | server %s, port %d", socket_server, socket_port)
        # set read timeout a little bit more than polling interval
        sock.settimeout(POLLING_INTERVAL+15)
        return sock

    def read(self):
        if self.conn is False:
            return ''
        ret = ''
        if self.type == 'serial':
            for _ in range(POLLING_INTERVAL+15):
                try:
                    ret = self.conn.read()
                except AttributeError as e_attr:
                    raise Exception('exception occured while reading serial') from e_attr
                except TypeError as e_type:
                    raise Exception('exception occured while reading serial') from e_type
                if len(ret) != 0:
                    break
        elif self.type == 'socket':
            ret = self.conn.recv(1)

        if len(ret) == 0:
            raise Exception('read byte errror')

        self.last_read_time = time.time()
        return ret

    def write(self, data):
        if self.conn is False:
            return False

        if self.last_read_time == 0:
            time.sleep(1)

        while time.time() - self.last_read_time < READ_WRITE_GAP:
            #logging.debug("pending write : time too short after last read")
            time.sleep(max([0, READ_WRITE_GAP - time.time() + self.last_read_time]))

        if self.type == 'serial':
            return self.conn.write(data)
        if self.type == 'socket':
            return self.conn.send(data)

        return False

    def close(self):
        ret = False
        if self.conn is not False:
            try:
                ret = self.conn.close()
                self.conn = False
            except:
                pass
        return ret

    def reconnect(self):
        self.close()
        while True:
            logging.info("[RS485] reconnecting to RS485...")
            if not self.connect():
                break
            time.sleep(10)


# hex parsing --------------------------------

def chksum(data_h):
    sum_buf = sum(bytearray.fromhex(data_h))
    # return chksum hex value in text format
    return '{0:02x}'.format(sum_buf % 256)

class Parser():
    def __init__(self, init_temp, light_count):
        self.init_temp = init_temp
        self.light_count = light_count

    def parse(self, hex_data):
        type_h = hex_data[4:7]
        seq_h = hex_data[7:8]
        dest_h = hex_data[10:14]
        src_h = hex_data[14:18]
        cmd_h = hex_data[18:20]
        value_h = hex_data[20:36]

        cmd = cmd_t_dic.get(cmd_h)
        if not cmd:
            cmd = cmd_h

        ret = {
            'header_h': hex_data[: 4],     # aa55
            'type_h': type_h,              # 30b(send) 30d(ack)
            'seq_h': seq_h,                # c(1st) d(2nd)
            'monitor_h': hex_data[8: 10],  # 00(wallpad) 02(KitchenTV)
            'dest_h': dest_h,              # 0100(wallpad0) 0e00(light0) 360X(thermoX)
            'src_h': src_h,
            'cmd_h': cmd_h,                # 3e(query)
            'value_h': value_h,
            'chksum_h': hex_data[36: 38],
            'trailer_h': hex_data[38: 42],
            'data_h': hex_data[4: 36],
            'payload_h': hex_data[18: 36],
            'type': type_t_dic.get(type_h),
            'seq': seq_t_dic.get(seq_h),
            'dest': device_t_dic.get(dest_h[: 2]),
            'dest_subid': str(int(dest_h[2: 4], 16)),
            'dest_room': room_t_dic.get(dest_h[2: 4]),
            'src': device_t_dic.get(src_h[: 2]),
            'src_subid': str(int(src_h[2: 4], 16)),
            'src_room': room_t_dic.get(src_h[2: 4]),
            'cmd': cmd,
            'value': value_h,
            'time': time.time(),
            'flag': None
        }
        return ret

    def thermo_parse(self, value):
        ret = {
            'heat_mode': 'heat' if value[:2] == '11' else 'off',
            'away': 'true' if value[2:4] == '01' else 'false',
            'set_temp': int(value[4:6], 16) if value[:2] == '11' else self.init_temp,
            'cur_temp': int(value[8:10], 16)
        }
        return ret

    def light_parse(self, value):
        ret = {}
        for i in range(1, self.light_count + 1):
            ret['light_'+str(i)] = 'off' if value[i*2-2:i*2] == '00' else 'on'
        return ret

    def fan_parse(self, value):
        preset_dic = {'40': 'Low', '80': 'Medium', 'c0': 'High'}
        state = 'off' if value[:2] == '10' else 'on'
        #state = 'off' if value[:2] == '00' else 'on'
        preset = 'Off' if state == 'off' else preset_dic.get(value[4:6])
        return {'state': state, 'preset': preset}



# mqtt functions ----------------------------

def init_mqttc(config):
    mqttc = mqtt.Client()
    mqttc.on_message = mqtt_on_message
    mqttc.on_subscribe = mqtt_on_subscribe
    mqttc.on_log = mqtt_on_log
    mqttc.on_connect = mqtt_on_connect
    mqttc.on_disconnect = mqtt_on_disconnect

    if config.get('MQTT','mqtt_allow_anonymous') != 'True':
        logtxt = "[MQTT] connecting (using username and password)"
        mqttc.username_pw_set(
            username=config.get('MQTT','mqtt_username',fallback=''),
            password=config.get('MQTT','mqtt_password',fallback=''))
    else:
        logtxt = "[MQTT] connecting (anonymous)"

    mqtt_server = config.get('MQTT','mqtt_server')
    mqtt_port = int(config.get('MQTT','mqtt_port'))
    for retry_cnt in range(1,31):
        try:
            logging.info(logtxt)
            mqttc.connect(mqtt_server, mqtt_port, 60)
            mqttc.loop_start()
            return mqttc
        except:
            logging.error("[MQTT] connection failure. #%d", retry_cnt)
            time.sleep(10)
    return False

# parse MQTT --> send hex packet
def mqtt_on_message(mqttc, userdata, msg):
    kocom = userdata
    command = msg.payload.decode('ascii')
    topic_d = msg.topic.split('/')

    # do not process other than command topic
    if topic_d[-1] != 'command':
        return

    logging.info("[MQTT RECV] %s %d %s", msg.topic, msg.qos, str(msg.payload))

    if 'thermo' in topic_d:
        mqtt_thermo(mqttc, kocom, command, topic_d)
    elif 'light' in topic_d:
        mqtt_light(mqttc, kocom, command, topic_d)
    elif 'gas' in topic_d:
        mqtt_gas(mqttc, kocom, command, topic_d)
    elif 'elevator' in topic_d:
        mqtt_elevator(mqttc, kocom, command, topic_d)
    elif 'fan' in topic_d:
        mqtt_fan(mqttc, kocom, command, topic_d)
    elif 'query' in topic_d:
        mqtt_query(mqttc, kocom, command, topic_d)

def mqtt_on_subscribe(_mqttc, _userdata, mid, granted_qos):
    logging.info("[MQTT] Subscribed: %d %d", mid, granted_qos)

def mqtt_on_log(_mqttc, _userdata, _level, string):
    logging.info("[MQTT] on_log : %s", string)

def mqtt_on_connect(mqttc, _userdata, _flags, ret_code):
    if ret_code == 0:
        logging.info("[MQTT] Connected - 0: OK")
        mqttc.subscribe('kocom/#', 0)
    else:
        logging.error("[MQTT] Connection error - %d: %s", ret_code,
                      mqtt.connack_string(ret_code))

def mqtt_on_disconnect(_mqttc, _userdata, ret_code=0):
    logging.error("[MQTT] Disconnected - %d", ret_code)

def mqtt_thermo(mqttc, kocom, command, topic_d):
    if 'heat_mode' in topic_d:
        # thermo heat/off : kocom/room/thermo/3/heat_mode/command
#        heatmode_dic = {'heat': '11', 'off': '01'}
        heatmode_dic = {'heat': '11', 'off': '00'}

        dev_id = device_h_dic['thermo']+'{0:02x}'.format(int(topic_d[3]))
        val = kocom.query(mqttc, dev_id)
        #settemp_hex = val['value'][4:6] if val['flag']!=False else '14'
        if val['flag'] is not False:
            settemp_hex = '{0:02x}'.format(int(kocom.config.get('User', 'init_temp')))
        else:
            settemp_hex = '14'
        value = heatmode_dic.get(command) + '00' + settemp_hex + '0000000000'
        kocom.send_wait_response(mqttc, dest=dev_id, value=value, log='thermo heatmode')
    elif 'set_temp' in topic_d:
        # thermo set temp : kocom/room/thermo/3/set_temp/command
        dev_id = device_h_dic['thermo']+'{0:02x}'.format(int(topic_d[3]))
        settemp_hex = '{0:02x}'.format(int(float(command)))

        value = '1100' + settemp_hex + '0000000000'
        kocom.send_wait_response(mqttc, dest=dev_id, value=value, log='thermo settemp')

def mqtt_light(mqttc, kocom, command, topic_d):
    # light on/off : kocom/livingroom/light/1/command
    dev_id = device_h_dic['light'] + room_h_dic.get(topic_d[1])
    value = kocom.query(mqttc, dev_id)['value']
    onoff_hex = 'ff' if command == 'on' else '00'
    light_id = int(topic_d[3])

    # turn on/off multiple lights at once : e.g) kocom/livingroom/light/12/command
    if light_id > 0:
        while light_id > 0:
            rem = light_id % 10
            value = value[:rem*2-2] + onoff_hex + value[rem*2:]
            kocom.send_wait_response(mqttc, dest=dev_id, value=value, log='light')
            light_id = int(light_id/10)
    else:
        kocom.send_wait_response(mqttc, dest=dev_id, value=value, log='light')

def mqtt_gas(mqttc, kocom, command, topic_d):
    # gas off : kocom/livingroom/gas/command
    dev_id = device_h_dic['gas'] + room_h_dic.get(topic_d[1])
    if command == 'off':
        kocom.send_wait_response(mqttc, dest=dev_id, cmd=cmd_h_dic.get(command), log='gas')
    else:
        logging.info("You can only turn off gas.")

def mqtt_elevator(mqttc, kocom, command, topic_d):
    # elevator on/off : kocom/myhome/elevator/command
    dev_id = device_h_dic['elevator'] + room_h_dic.get(topic_d[1])
    state_on = json.dumps({'state': 'on'})
    state_off = json.dumps({'state': 'off'})
    elevator_state_t = "kocom/myhome/elevator/state"
    if command == 'on':
        ret_elevator = None
        if kocom.config.get('Elevator', 'type', fallback='rs485') == 'rs485':
            ret_elevator = kocom.send(dev_id, cmd_h_dic['on'], '0'*16, 'elevator', False)
        elif kocom.config.get('Elevator', 'type', fallback='rs485') == 'tcpip':
            ret_elevator = kocom.call_elevator_tcpip()

        if ret_elevator is None:
            logging.debug("elevator send failed")
            return

        threading.Thread(target=mqttc.publish, args=(elevator_state_t, state_on)).start()
        if kocom.config.get('Elevator', 'rs485_floor', fallback=None) is None:
            threading.Timer(5, mqttc.publish, args=(elevator_state_t, state_off)).start()
    elif command == 'off':
        threading.Thread(target=mqttc.publish, args=(elevator_state_t, state_off)).start()

def mqtt_fan(mqttc, kocom, command, topic_d):
    if 'set_preset_mode' in topic_d:
        # kocom/livingroom/fan/set_preset_mode/command
        dev_id = device_h_dic['fan'] + room_h_dic.get(topic_d[1])
        onoff_dic = {'off':'1000', 'on':'1100'}  #onoff_dic = {'off':'0000', 'on':'1101'}
        speed_dic = {'Off':'00', 'Low':'40', 'Medium':'80', 'High':'c0'}
        if command == 'Off':
            onoff = onoff_dic['off']
        elif command in speed_dic: # fan on with specified speed
            onoff = onoff_dic['on']

        speed = speed_dic.get(command)
        value = onoff + speed + '0'*10
        kocom.send_wait_response(mqttc, dest=dev_id, value=value, log='fan')
    else:
        # kocom/livingroom/fan/command
        dev_id = device_h_dic['fan'] + room_h_dic.get(topic_d[1])
        onoff_dic = {'off':'1000', 'on':'1100'}  #onoff_dic = {'off':'0000', 'on':'1101'}
        speed_dic = {'Low':'40', 'Medium':'80', 'High':'c0'}
        init_fan_mode = kocom.config.get('User', 'init_fan_mode')
        if command in onoff_dic: # fan on off with previous speed
            onoff = onoff_dic.get(command)
            #value = query(dev_id)['value']
            #speed = value[4:6]
            speed = speed_dic.get(init_fan_mode)

        value = onoff + speed + '0'*10
        kocom.send_wait_response(mqttc, dest=dev_id, value=value, log='fan')

def mqtt_query(_mqttc, kocom, command, _topic_d):
    # kocom/myhome/query/command
    if command == 'PRESS':
        kocom.poll_state(enforce=True)

# ===== parse hex packet --> publish MQTT =====

def publish_status(mqttc, parser, config, pkt):
    threading.Thread(target=packet_processor, args=(mqttc, parser, config, pkt)).start()

def packet_processor(mqttc, parser, config, pkt):
    logtxt = ""
    if pkt['type'] == 'send' and pkt['dest'] == 'wallpad':  # response packet to wallpad
        if pkt['src'] == 'thermo' and pkt['cmd'] == 'state':
            state = parser.thermo_parse(pkt['value'])
            logtxt = '[MQTT publish|thermo] id[{}] data[{}]'.format(pkt['src_subid'], state)
            mqttc.publish("kocom/room/thermo/" + pkt['src_subid'] + "/state", json.dumps(state))
        elif pkt['src'] == 'light' and pkt['cmd'] == 'state':
            state = parser.light_parse(pkt['value'])
            logtxt = '[MQTT publish|light] room[{}] data[{}]'.format(pkt['src_room'], state)
            mqttc.publish("kocom/{}/light/state".format(pkt['src_room']), json.dumps(state))
        elif pkt['src'] == 'fan' and pkt['cmd'] == 'state':
            state = parser.fan_parse(pkt['value'])
            logtxt = '[MQTT publish|fan] data[{}]'.format(state)
            mqttc.publish("kocom/livingroom/fan/state", json.dumps(state))
        elif pkt['src'] == 'gas':
            state = {'state': pkt['cmd']}
            logtxt = '[MQTT publish|gas] data[{}]'.format(state)
            mqttc.publish("kocom/livingroom/gas/state", json.dumps(state))
    elif pkt['type'] == 'send' and pkt['dest'] == 'elevator':
        floor = int(pkt['value'][2:4], 16)
        rs485_floor = int(config.get('Elevator','rs485_floor', fallback=0))
        if rs485_floor != 0:
            state = {'floor': floor}
            if rs485_floor == floor:
                state['state'] = 'off'
        else:
            state = {'state': 'off'}
        logtxt='[MQTT publish|elevator] data[{}]'.format(state)
        mqttc.publish("kocom/myhome/elevator/state", json.dumps(state))
        # aa5530bc0044000100010300000000000000350d0d

    if logtxt != "" and config.get('Log', 'show_mqtt_publish') == 'True':
        logging.info(logtxt)


# ===== publish MQTT Devices Discovery =====
# refer: https://www.home-assistant.io/docs/mqtt/discovery/
# <discovery_prefix>/<component>/<object_id>/config

def discovery(mqttc, config):
    dev_publisher = {
        'fan': publish_fan,
        'gas': publish_gas,
        'elevator': publish_elevator,
        'light': publish_light,
        'thermo': publish_thermo,
        'query': publish_query}

    verbose = False
    show_mqtt_publish = config.get('Log', 'show_mqtt_publish')
    if show_mqtt_publish == 'True':
        verbose = True

    dev_list = [x.strip() for x in config.get('Device','enabled').split(',')]
    for dev_token in dev_list:
        dev = dev_token.split('_')
        sub = ''
        if len(dev) > 1:
            sub = dev[1]

        func = dev_publisher[dev[0]]
        if func:
            func(mqttc, sub, config, verbose)

    dev_publisher['query'](mqttc, sub, config, verbose)

def publish_fan(mqttc, _sub, _config, verbose):
    dev = 'fan'
    topic = 'homeassistant/fan/kocom_wallpad_fan/config'
    payload = {
        'name': 'Kocom Wallpad Fan',
        'cmd_t': 'kocom/livingroom/fan/command',
        'stat_t': 'kocom/livingroom/fan/state',
        'stat_val_tpl': '{{ value_json.state }}',
        'pr_mode_stat_t': 'kocom/livingroom/fan/state',
        'pr_mode_val_tpl': '{{ value_json.preset }}',
        'pr_mode_cmd_t': 'kocom/livingroom/fan/set_preset_mode/command',
        'pr_mode_cmd_tpl': '{{ value }}',
        'pr_modes': ['Off', 'Low', 'Medium', 'High'],
        'pl_on': 'on',
        'pl_off': 'off',
        'qos': 0,
        'uniq_id': '{}_{}_{}'.format('kocom', 'wallpad', dev),
        'device': {
            'name': '코콤 스마트 월패드',
            'ids': 'kocom_smart_wallpad',
            'mf': 'KOCOM',
            'mdl': '스마트 월패드',
            'sw': SW_VERSION
        }
    }
    logtxt='[MQTT Discovery|{}] data[{}]'.format(dev, topic)
    mqttc.publish(topic, json.dumps(payload))
    if verbose:
        logging.info(logtxt)

def publish_gas(mqttc, _sub, _config, verbose):
    dev = 'gas'
    topic = 'homeassistant/switch/kocom_wallpad_gas/config'
    payload = {
        'name': 'Kocom Wallpad Gas',
        'cmd_t': 'kocom/livingroom/gas/command',
        'stat_t': 'kocom/livingroom/gas/state',
        'val_tpl': '{{ value_json.state }}',
        'pl_on': 'on',
        'pl_off': 'off',
        'ic': 'mdi:gas-cylinder',
        'qos': 0,
        'uniq_id': '{}_{}_{}'.format('kocom', 'wallpad', dev),
        'device': {
            'name': '코콤 스마트 월패드',
            'ids': 'kocom_smart_wallpad',
            'mf': 'KOCOM',
            'mdl': '스마트 월패드',
            'sw': SW_VERSION
        }
    }
    logtxt='[MQTT Discovery|{}] data[{}]'.format(dev, topic)
    mqttc.publish(topic, json.dumps(payload))
    if verbose:
        logging.info(logtxt)

def publish_elevator(mqttc, _sub, _config, verbose):
    dev = 'elevator'
    topic = 'homeassistant/switch/kocom_wallpad_elevator/config'
    payload = {
        'name': 'Kocom Wallpad Elevator',
        'cmd_t': "kocom/myhome/elevator/command",
        'stat_t': "kocom/myhome/elevator/state",
        'val_tpl': "{{ value_json.state }}",
        'pl_on': 'on',
        'pl_off': 'off',
        'ic': 'mdi:elevator',
        'qos': 0,
        'uniq_id': '{}_{}_{}'.format('kocom', 'wallpad', dev),
        'device': {
            'name': '코콤 스마트 월패드',
            'ids': 'kocom_smart_wallpad',
            'mf': 'KOCOM',
            'mdl': '스마트 월패드',
            'sw': SW_VERSION
        }
    }
    logtxt='[MQTT Discovery|{}] data[{}]'.format(dev, topic)
    mqttc.publish(topic, json.dumps(payload))
    if verbose:
        logging.info(logtxt)

def publish_light(mqttc, sub, config, verbose):
    dev = 'light'
    for num in range(1, int(config.get('User', 'light_count'))+1):
        topic = 'homeassistant/light/kocom_{}_light{}/config'.format(sub, num)
        payload = {
            'name': 'Kocom {} Light{}'.format(sub, num),
            'cmd_t': 'kocom/{}/light/{}/command'.format(sub, num),
            'stat_t': 'kocom/{}/light/state'.format(sub),
            'stat_val_tpl': '{{ value_json.light_' + str(num) + ' }}',
            'pl_on': 'on',
            'pl_off': 'off',
            'qos': 0,
            'uniq_id': '{}_{}_{}{}'.format('kocom', 'wallpad', dev, num),
            'device': {
                'name': '코콤 스마트 월패드',
                'ids': 'kocom_smart_wallpad',
                'mf': 'KOCOM',
                'mdl': '스마트 월패드',
                'sw': SW_VERSION
            }
        }
        logtxt='[MQTT Discovery|{}{}] data[{}]'.format(dev, num, topic)
        mqttc.publish(topic, json.dumps(payload))
        if verbose:
            logging.info(logtxt)

def publish_thermo(mqttc, sub, _config, verbose):
    dev = 'thermo'
    num = int(room_h_dic.get(sub))
    topic = 'homeassistant/climate/kocom_{}_thermostat/config'.format(sub)
    payload = {
        'name': 'Kocom {} Thermostat'.format(sub),
        'mode_cmd_t': 'kocom/room/thermo/{}/heat_mode/command'.format(num),
        'mode_stat_t': 'kocom/room/thermo/{}/state'.format(num),
        'mode_stat_tpl': '{{ value_json.heat_mode }}',

        'temp_cmd_t': 'kocom/room/thermo/{}/set_temp/command'.format(num),
        'temp_stat_t': 'kocom/room/thermo/{}/state'.format(num),
        'temp_stat_tpl': '{{ value_json.set_temp }}',

        'curr_temp_t': 'kocom/room/thermo/{}/state'.format(num),
        'curr_temp_tpl': '{{ value_json.cur_temp }}',
        'modes': ['off', 'heat'],
        'min_temp': 20,
        'max_temp': 30,
        'ret': 'false',
        'qos': 0,
        'uniq_id': '{}_{}_{}{}'.format('kocom', 'wallpad', dev, num),
        'device': {
            'name': '코콤 스마트 월패드',
            'ids': 'kocom_smart_wallpad',
            'mf': 'KOCOM',
            'mdl': '스마트 월패드',
            'sw': SW_VERSION
        }
    }
    logtxt='[MQTT Discovery|{}{}] data[{}]'.format(dev, num, topic)
    mqttc.publish(topic, json.dumps(payload))
    if verbose:
        logging.info(logtxt)

def publish_query(mqttc, _sub, _config, verbose):
    dev = 'query'
    topic = 'homeassistant/button/kocom_wallpad_query/config'
    payload = {
        'name': 'Kocom Wallpad Query',
        'cmd_t': 'kocom/myhome/query/command',
        'qos': 0,
        'uniq_id': '{}_{}_{}'.format('kocom', 'wallpad', dev),
        'device': {
            'name': '코콤 스마트 월패드',
            'ids': 'kocom_smart_wallpad',
            'mf': 'KOCOM',
            'mdl': '스마트 월패드',
            'sw': SW_VERSION
        }
    }
    logtxt='[MQTT Discovery|{}] data[{}]'.format(dev, topic)
    mqttc.publish(topic, json.dumps(payload))
    if verbose:
        logging.info(logtxt)

class Kocom():
    config = None
    rs485 = None
    mqttc = None
    parser = None
    msg_q = queue.Queue(BUF_SIZE)
    ack_q = queue.Queue(1)
    ack_data = []
    wait_q = queue.Queue(1)
    wait_target = queue.Queue(1)
    send_lock = threading.Lock()
    cache_data = []
    thread_list = []
    poll_timer = None

    def __init__(self):
        logging.basicConfig(format='%(levelname)s[%(asctime)s]:%(message)s ', level=logging.DEBUG)

        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        self.config = config

        if config.get('RS485', 'type') == 'serial':
            self.rs485 = RS485Wrapper(
                serial_port = config.get('RS485', 'serial_port', fallback=None))
        elif config.get('RS485', 'type') == 'socket':
            self.rs485 = RS485Wrapper(
                socket_server = config.get('RS485', 'socket_server'),
                socket_port = int(config.get('RS485', 'socket_port')))
        else:
            logging.error("[CONFIG] invalid [RS485] config: use \"serial\" or \"socket\". exit")
            sys.exit(1)
        if not self.rs485.connect():
            logging.error("[RS485] connection error. exit")
            sys.exit(1)

        init_temp = int(config.get('User', 'init_temp'))
        light_count = int(config.get('User', 'light_count'))
        self.parser = Parser(init_temp, light_count)

        self.mqttc = init_mqttc(config)
        if not self.mqttc:
            logging.error("[MQTT] conection error. exit")
            sys.exit(1)

        self.thread_list.append(
            threading.Thread(
                target=self.read_serial,
                name='read_serial'))
        self.thread_list.append(
            threading.Thread(
                target=self.listen_hexdata,
                name='listen_hexdata'))

    def run(self):
        self.mqttc.user_data_set(self)

        for thread_instance in self.thread_list:
            thread_instance.start()

        poll_timer = threading.Timer(1, self.poll_state)
        poll_timer.start()

        discovery(self.mqttc, self.config)

    def listen_hexdata(self):
        while True:
            hexdata = self.msg_q.get()

            if self.config.get('Log', 'show_recv_hex') == 'True':
                logging.info("[recv] %s", hexdata)

            pkt = self.parser.parse(hexdata)

            # store recent packets in cache
            self.cache_data.insert(0, pkt)
            if len(self.cache_data) > BUF_SIZE:
                del self.cache_data[-1]

            if pkt['data_h'] in self.ack_data:
                self.ack_q.put(hexdata)
                continue

            if not self.wait_target.empty():
                if pkt['dest_h'] == self.wait_target.queue[0] and pkt['type'] == 'ack':
                    if len(self.ack_data) != 0:
                        logging.info("[ACK] responce packet received before ACK. Assuming ACK OK")
                        self.ack_q.put(hexdata)
                        time.sleep(0.5)
                    self.wait_q.put(pkt)
                    continue

            publish_status(self.mqttc, self.parser, self.config, pkt)

    # ===== thread functions =====
    def poll_state(self, enforce=False):
        self.poll_timer.cancel()

        dev_list = [x.strip() for x in self.config.get('Device','enabled').split(',')]
        no_polling_list = ['wallpad', 'elevator']

        #thread health check
        for thread_instance in self.thread_list:
            if not thread_instance.is_alive():
                logging.error("[THREAD] %s is not active. starting.", thread_instance.name)
                thread_instance.start()

        for dev_token in dev_list:
            dev = dev_token.split('_')
            if dev[0] in no_polling_list:
                continue

            dev_id = device_h_dic.get(dev[0])
            if len(dev) > 1:
                sub_id = room_h_dic.get(dev[1])
            else:
                sub_id = '00'

            if dev_id is not None and sub_id is not None:
                val = self.query(dev_id + sub_id, publish=True, enforce=enforce)
                if val and val['flag']:
                    break
                time.sleep(1)

        self.poll_timer.cancel()
        self.poll_timer = threading.Timer(POLLING_INTERVAL, self.poll_state)
        self.poll_timer.start()


    def read_serial(self):
        buf = ''
        not_parsed_buf = ''
        while True:
            try:
                data = self.rs485.read()
                hex_d = '{0:02x}'.format(ord(data))

                buf += hex_d
                if buf[:len(HEADER_H)] != HEADER_H[:len(buf)]:
                    not_parsed_buf += buf
                    buf=''
                    frame_start = not_parsed_buf.find(HEADER_H, len(HEADER_H))
                    if frame_start < 0:
                        continue
                    not_parsed_buf = not_parsed_buf[:frame_start]
                    buf = not_parsed_buf[frame_start:]

                if not_parsed_buf != '':
                    logging.info("[comm] not parsed %s", not_parsed_buf)
                    not_parsed_buf = ''

                if len(buf) == (PACKET_SIZE * 2):
                    chksum_calc = chksum(buf[len(HEADER_H):CHKSUM_POSITION*2])
                    chksum_buf = buf[CHKSUM_POSITION*2:CHKSUM_POSITION*2+2]
                    if chksum_calc == chksum_buf and buf[-len(TRAILER_H):] == TRAILER_H:
                        if self.msg_q.full():
                            # probably error occured while running listen_hexdata thread
                            logging.error("msg_q is full. please manually restart the program.")
                        self.msg_q.put(buf)  # valid packet
                        buf=''
                    else:
                        logging.info("[comm] invalid packet %s expected checksum %s",
                                     buf, chksum_calc)
                        frame_start = buf.find(HEADER_H, len(HEADER_H))
                        # if there's header packet in the middle of invalid packet,
                        # re-parse from that posistion
                        if frame_start < 0:
                            not_parsed_buf += buf
                            buf=''
                        else:
                            not_parsed_buf += buf[:frame_start]
                            buf = buf[frame_start:]
            except Exception as exc:
                logging.error("*** Read error.[%s]", exc)
                self.poll_timer.cancel()
                del self.cache_data[:]
                self.rs485.reconnect()
                self.poll_timer = threading.Timer(2, self.poll_state)
                self.poll_timer.start()

    # query device --------------------------
    def query(self, device_h, publish=False, enforce=False):
        # find from the cache first
        for val in self.cache_data:
            if enforce:
                break
            # if there's no data within polling interval, then exit cache search
            if (time.time() - val['time']) > POLLING_INTERVAL:
                break
            if (val['type'] == 'ack' and val['src'] == 'wallpad' and
                    val['dest_h'] == device_h and val['cmd'] != 'query'):
                if self.config.get('Log', 'show_query_hex') == 'True':
                    logging.info("[cache|%s%s] query cache %s",
                                 val['dest'], val['dest_subid'], val['data_h'])
                return val  # return the value in the cache

        # if there's no cache data within polling inteval, then send query packet
        if self.config.get('Log', 'show_query_hex') == 'True':
            log = 'query ' + device_t_dic.get(device_h[:2]) + str(int(device_h[2:4],16))
        else:
            log = None
        return self.send_wait_response(dest=device_h, cmd=cmd_h_dic['query'],
                                       log=log, publish=publish)


    def send_wait_response(self, dest, cmd=cmd_h_dic['state'],
                           value='0'*16, log=None,
                           check_ack=True, publish=True):
        #logging.debug("waiting for send_wait_response : %s", dest)
        self.wait_target.put(dest)
        #logging.debug("entered send_wait_response : %s", dest)
        ret = {'value':'0'*16, 'flag':False}

        if self.send(dest, cmd, value, log, check_ack):
            try:
                ret = self.wait_q.get(True, 2)
                if publish:
                    publish_status(self.mqttc, self.parser, self.config, ret)
            except queue.Empty:
                pass
        self.wait_target.get()
        #logging.debug("exiting send_wait_response : %s", dest)
        return ret

    def send(self, dest, cmd, value, log=None, check_ack=True):
        src = device_h_dic['wallpad'] + '00'

        # TBD: fine-grained lock
        with self.send_lock:
            self.ack_data.clear()
            ret = False
            # if there's no ACK received, then repeat sending with next sequence code
            for seq_h in seq_t_dic:
                payload = type_h_dic['send'] + seq_h + '00' + dest + src + cmd + value
                send_data = HEADER_H + payload + chksum(payload) + TRAILER_H
                try:
                    if not self.rs485.write(bytearray.fromhex(send_data)):
                        raise Exception('Not ready')
                except Exception as exc:
                    logging.error("[RS485] Write error.[%s]", exc)
                    break

                if log:
                    logging.info("[SEND|%s] %s", log, send_data)

                if not check_ack:
                    time.sleep(1)
                    ret = send_data
                    break

                # wait and checking for ACK
                self.ack_data.append(type_h_dic['ack'] + seq_h + '00' + src + dest + cmd + value)
                try:
                    # random wait between 1.3~1.5 seconds for ACK
                    wait_time = 1.3 + (0.2 * random.random())
                    self.ack_q.get(True, wait_time)
                    if self.config.get('Log', 'show_recv_hex') == 'True':
                        logging.info("[ACK] OK")
                    ret = send_data
                    break
                except queue.Empty:
                    pass

            if ret is False:
                logging.info("[RS485] send failed. closing RS485. it will retry soon.")
                self.rs485.close()

            self.ack_data.clear()
        return ret

    # ===== elevator call via TCP/IP =====
    def call_elevator_tcpip(self):
        sock = socket.socket()
        sock.settimeout(10)

        apt_server = self.config.get('Elevator', 'tcpip_apt_server')
        apt_port = int(self.config.get('Elevator', 'tcpip_apt_port'))

        try:
            sock.connect((apt_server, apt_port))
        except Exception as exc:
            logging.error("Apartment server socket connection failure : %s | server %s, port %d",
                          exc, apt_server, apt_port)
            return False

        logging.info("Apartment server socket connected | server %s, port %d", apt_server, apt_port)

        try:
            sock.send(bytearray.fromhex(self.config.get('Elevator', 'tcpip_packet1')))
            rcv = sock.recv(512)
            rcv_hexstr = ''.join("%02x" % i for i in rcv)
            logging.info("recv from apt server: %s", rcv_hexstr)
            time.sleep(0.1)
            sock.send(bytearray.fromhex(self.config.get('Elevator', 'tcpip_packet2')))
            rcv = sock.recv(512)
            rcv_hexstr = ''.join("%02x" % i for i in rcv)
            logging.info("recv from apt server: %s", rcv_hexstr)
            sock.send(bytearray.fromhex(self.config.get('Elevator', 'tcpip_packet3')))
            for _ in range(100):
                rcv = sock.recv(512)
                if len(rcv) == 0:
                    logging.info("apt server connection closed by peer")
                    sock.close()
                    return True
                rcv_hex = ''.join("%02x" % i for i in rcv)
                logging.info("recv from apt server: %s", rcv_hex)
                if rcv_hex == self.config.get('Elevator', 'tcpip_packet4'):
                    logging.info("elevator arrived. sending last heartbeat")
                    break
            sock.send(bytearray.fromhex(self.config.get('Elevator', 'tcpip_packet2')))
            rcv = sock.recv(512)
            rcv_hexstr = ''.join("%02x" % i for i in rcv)
            logging.info("recv from apt server: %s", rcv_hexstr)
            sock.close()
        except Exception as exc:
            logging.error("Apartment server socket communication failure : %s", exc)
            return False

        return True

if __name__ == "__main__":
    Kocom().run()
