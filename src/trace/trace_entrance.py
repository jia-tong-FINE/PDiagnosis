import json

import requests
from kafka import KafkaConsumer

from .trace_based_anomaly_detection import TraceModel
from .trace_parser import TraceParser


def trace_based_anomaly_detection_entrance(config):
    # initiation
    CONSUMER = KafkaConsumer(config["trace_topic"],
                             bootstrap_servers=config["kafka_address"],
                             auto_offset_reset='latest',
                             enable_auto_commit=False,
                             security_protocol='PLAINTEXT')

    trace_parser = TraceParser()
    trace_model = TraceModel()
    print('Trace Monitor Running')
    for message in CONSUMER:
        data = json.loads(message.value.decode('utf8'))
        if 'trace' in message.topic:
            print(data['value'])
            complete_flag = trace_parser.update_trace_info(data)
            if complete_flag == 1:

                trace_model.update_model_pattern(trace_parser)
                trace_model.add_to_q()
                # print(json.dumps(trace_model.model))
                if trace_model.fixed_trace_q.full():
                    main_key, anomaly_time = trace_model.anomaly_detection_with_queue(data['timestamp'])
                    if main_key != 'null':
                        send_dict = {'cmdb_id': main_key, 'timestamp': anomaly_time}

                        requests.post('http://127.0.0.1:' + str(config['decision_port']) + '/trace',
                                      json.dumps(send_dict))
