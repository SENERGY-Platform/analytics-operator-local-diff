#  Copyright 2019 InfAI (CC SES)
#  #
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  #
#    http://www.apache.org/licenses/LICENSE-2.0
#  #
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import json
import os

import paho.mqtt.client as mqtt
from jsonpath_rw import jsonpath
from jsonpath_rw_ext import parse

# for calling extended methods
import jsonpath_rw_ext as jp


CONFIG = json.loads(os.getenv("CONFIG"))
TOPICS = json.loads(os.getenv("INPUT"))

print(CONFIG)
print(TOPICS)

test = jp.match1("$.[0].mappings[0].source", TOPICS)

val0 = 0


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe(TOPICS[0]["name"])


def on_message(client, userdata, msg):
    global val0
    message = msg.payload.decode('utf8').replace('"{', '{').replace('"}', '}').replace('\\', '')
    js = json.loads(message)
    val = get_value(js)
    diff = val - val0
    val0 = val
    x = {
        "pipelineId": get_config_value("pipelineId"),
        "operatorId": get_config_value("operatorId"),
        "analytics": {
            "diff": diff
        }
    }
    client.publish(get_config_value("outputTopic"), payload=json.dumps(x), qos=0, retain=False)


def get_value(js):
    return jp.match1(get_value_path(), js)


def get_value_path():
    return jp.match1("$.[0].mappings[0].source", TOPICS)


def get_config_value(value):
    return jp.match1(value, CONFIG)


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(os.getenv("BROKER_HOST", "localhost"), int(os.getenv("BROKER_PORT", 1883)), 60)

client.loop_forever()
