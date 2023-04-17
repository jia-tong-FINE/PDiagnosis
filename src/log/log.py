import json

import requests
from kafka import KafkaConsumer

template_dict = {}


def log_based_anomaly_detection_entrance(config):
    # initiation
    CONSUMER = KafkaConsumer(config["log_topic"],
                             bootstrap_servers=config["kafka_address"],
                             auto_offset_reset='latest',
                             enable_auto_commit=False,
                             security_protocol='PLAINTEXT')
    print('Log Monitor Running')
    for message in CONSUMER:
        data = json.loads(message.value.decode('utf8'))
        if config['log_keyword'] in data["value"]:
            send_dict = {"cmdb_id": data["cmdb_id"], "timestamp": data["timestamp"], "logname": data["logname"]}
            requests.post('http://127.0.0.1:' + str(config['decision_port']) + '/log', json.dumps(send_dict))
