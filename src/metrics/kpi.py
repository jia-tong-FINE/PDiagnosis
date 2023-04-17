import json

from kafka import KafkaConsumer

CONSUMER = KafkaConsumer('a-kpi',
                         bootstrap_servers=['10.3.2.41', '10.3.2.4', '10.3.2.36'],
                         auto_offset_reset='latest',
                         enable_auto_commit=False,
                         security_protocol='PLAINTEXT')

kpi = {}
cmdb = []
first = True


# 主要函数，实时消费kafka并更新模型，预测结果
# Thread入口
def kpi_monitor():
    print('KPI Consumer')
    i = 0
    for message in CONSUMER:
        i += 1
        data = json.loads(message.value.decode('utf8'))
        timestamp = data['timestamp']
        print(i, message.topic, timestamp)
        # 将数据写入内存
