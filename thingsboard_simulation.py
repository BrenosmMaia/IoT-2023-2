import paho.mqtt.client as paho
import json
import random
import time
import threading

controls = {
        "sala_1": {
            "temperature_device": ["2DREgrACfrXggP1usqWq", True],
            "lux_device": ["1YcqNNH8kUlQgr1SHGQ0", False],
        },
        "sala_2": {
            "temperature_device": ["9iKswlE9JNCON7QKnjDm", True],
            "lux_device": ["27ZefQdr5NNuYj9D2Z92", False],
        },
        "sala_3": {
            "temperature_device": ["bXmC72VicQnUdBUXs5Bd", True],
            "lux_device": ["NzGmBgmQiE6UkpGLxKxE", False],
        },
        "all": {
            "period_sleep": ["BpSgDLnMD0HtpqC9l5Ch", 5]
        }
    }

def config_device(broker, port, device_id, ACCESS_TOKEN):
    client = paho.Client(client_id=device_id)
    client.on_publish = on_publish
    client.username_pw_set(ACCESS_TOKEN)
    client.connect(broker, port, keepalive=60)
    return client

def on_publish(client, userdata, result):
    print("\ndata published to thingsboard")

def send_data(client, telemetry_data):
    payload = json.dumps(telemetry_data)
    ret = client.publish("v1/devices/me/telemetry", payload)
    print(payload)

def generate_temperature(control):
    if control:
      return round(random.uniform(18, 23))
    else:
      return round(random.uniform(24, 35))

def generate_decibels():
    return round(random.uniform(50, 93))

def generate_lux(control):
    if control:
       return round(random.uniform(600, 700))
    else:
      return round(random.uniform(300, 500))

def create_clients(devices_tokens, broker, port):
    clients = {}
    for room, devices in devices_tokens.items():
        clients[room] = {}
        for device_type, token in devices.items():
            device_id = f"{room}_{device_type}"
            clients[room][device_type] = config_device(broker, port, device_id, token)

    return clients

class RpcClient:
    def __init__(self, token, broker, port, room , device_type):
        self.client = paho.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.username_pw_set(token)
        self.client.connect(broker, port, 60)
        self.room = room
        self.device_type = device_type

    def on_connect(self, client, userdata, flags, rc):
        client.subscribe('v1/devices/me/rpc/request/+')

    def on_message(self, client, userdata, msg):
        global controls

        if msg.topic.startswith('v1/devices/me/rpc/request/'):
            requestId = msg.topic[len('v1/devices/me/rpc/request/'):len(msg.topic)]
            data = json.loads(msg.payload)
            if data['method'] == 'setValue':
                params = data['params']
                controls[self.room][self.device_type][1] = params
                print(f"\n***RPC RECEBIDO: {params}, Sala: {self.room}, Dispositivo: {self.device_type}***")

    def start(self):
        self.client.loop_start()

    def stop(self):
        self.client.disconnect()

def send_loop(clients, controls):
    for i in range(1000):
        print(f"\n***ITERACAO {i}***")
        print(f"***SLEEP {controls['all']['period_sleep'][1]}***")
        for sala in ['sala_1', 'sala_2', 'sala_3']:
            send_data(clients[sala]['temperature_device'], {"temperature": generate_temperature(controls[sala]["temperature_device"][1])})
            send_data(clients[sala]['decibels_device'], {"dB": generate_decibels()})
            send_data(clients[sala]['lux_device'], {"lx": generate_lux(controls[sala]["lux_device"][1])})
        time.sleep(controls['all']['period_sleep'][1])

def main():
    broker = 'dev-thingsboard.lcc.ime.uerj.br'
    port = 1883

    devices_tokens = {
        "sala_1": {
            "temperature_device": "43eKWGMm7vInTy3AoMrP",
            "decibels_device": "RTNDQhhS01j4PhcEhS5k",
            "lux_device": "7e3YBJjqJSCbi3PvUon0",
        },
        "sala_2": {
            "temperature_device": "mVvhBYFmbZQhRxSJdtxm",
            "decibels_device": "G0ptqGQOLrcl1uopcIfR",
            "lux_device": "KC0Nh5OFi1DSr125Uans",
        },
        "sala_3": {
            "temperature_device": "Gp9Z3HlGX2w0awNLN8wh",
            "decibels_device": "neR8yoOviKSosM4K81O3",
            "lux_device": "g94p1vlviFQ8fpGtS2tz",
        }
    }

    clients = create_clients(devices_tokens, broker, port)
    rpc_clients = []

    for sala, devices in controls.items():
      for device, (id, _) in devices.items():
        if id != "":
            rpc_clients.append(RpcClient(id, broker, port, sala, device))

    sleep_period_thread = RpcClient(controls["all"]["period_sleep"][0], broker, port, "all", "period_sleep")
    sleep_period_thread.start()

    send_thread = threading.Thread(target=send_loop, args=(clients, controls))
    send_thread.start()

    try:
        for rpc_client in rpc_clients:
            threading.Thread(target=rpc_client.start).start()
        while True:
            pass

    except KeyboardInterrupt:
        for rpc_client in rpc_clients:
            rpc_client.stop()
        sleep_period_thread.stop()

main()