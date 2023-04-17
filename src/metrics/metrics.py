import json
import time

import requests
from kafka import KafkaConsumer

from util import detection

# Metrics异常检测模块
# Author: Midor (midor@pku.edu.cn)

metric = {}
cmdb = []
metric_name = []
first = True
anomaly_metric = {}
config = {}


def to_string_date(now_stamp):
    t = time.localtime(now_stamp)
    return time.strftime("%Y-%m-%d %H:%M:%S", t)


# 对不同大类型指标应用不同检测策略
def is_normal_kpi_type(kpi_name):
    ignores = config['kpi_ignore_keyword']
    for ignore in ignores:
        if ignore in kpi_name:
            return False
    return True


# 将检测到的异常发送给Decision Maker
def send_to_decision_maker(data):
    requests.post('http://127.0.0.1:' + str(config['decision_port']) + '/metric', json.dumps(data))


# 将获得的数据写入内存
def metric_stash(data):
    cmdb_id = data['cmdb_id']
    kpi_name = data['kpi_name']
    # 假设按时间顺序下发数据
    metric[cmdb_id][kpi_name].append(data['value'])
    while len(metric[cmdb_id][kpi_name]) > 15:
        del metric[cmdb_id][kpi_name][0]


# 进行异常检测
def anomaly_check(data):
    cmdb_id = data['cmdb_id']
    kpi_name = data['kpi_name']
    history_list = metric[cmdb_id][kpi_name][:]
    value = data['value']
    p_value = detection.kde_detect(history_list, value)
    sigma_value = detection.sigma_detect(history_list, value)
    not_in_range = detection.max_min_detect(history_list, value)
    change_mean = detection.history_mean_detect(history_list, value)
    error_value = detection.error_detect(history_list, value)
    # system-a IO相关指标经常出现长期0之后出现非0值的情况，根因多不包含与system.io相关，应当忽略
    # tcp指标需要设计方法
    # if cmdb_id == 'gjjha2' and kpi_name == 'system.net.bytes_sent':
    #     print(p_value, data)
    #     print(history_list)
    conclusion = p_value < config['kde_threshold'] and not_in_range and sigma_value and change_mean and error_value
    return p_value, conclusion  # 阈值之后调节


# 检查异常波动，防止长期报告同一异常
def long_term_check(data):
    cmdb_id = data['cmdb_id']
    kpi_name = data['kpi_name']
    timestamp = data['timestamp']
    anomaly_metric.setdefault(cmdb_id, {})
    anomaly_metric[cmdb_id].setdefault(kpi_name, [])
    anomaly_history = anomaly_metric[cmdb_id][kpi_name]
    history_length = len(anomaly_history)
    # 保存异常历史记录
    if history_length == 0 or (1 <= history_length <= 6 and anomaly_history[-1] + 60 == timestamp):
        anomaly_metric[cmdb_id][kpi_name].append(timestamp)
        return True
    # 如果指标异常连续的长于6分钟, 或有断点，则清空指标数据，重新学习
    anomaly_metric[cmdb_id][kpi_name] = []
    metric[cmdb_id][kpi_name] = []
    return False


# 主要函数，实时消费kafka并更新模型，预测结果
# Thread入口
def metric_monitor(conf):
    global config
    config = conf

    CONSUMER = KafkaConsumer(config["metric_topic"],
                             bootstrap_servers=config["kafka_address"],
                             auto_offset_reset='latest',
                             enable_auto_commit=False,
                             security_protocol='PLAINTEXT')

    print('Metric Monitor Running')

    for message in CONSUMER:
        data = json.loads(message.value.decode('utf8'))
        timestamp = data['timestamp']
        cmdb_id = data['cmdb_id']
        kpi_name = data['kpi_name']
        value = data['value']
        metric.setdefault(cmdb_id, {})
        metric[cmdb_id].setdefault(kpi_name, [])
        history_list = metric[cmdb_id][kpi_name][:]
        data_enough = len(history_list) >= 15
        if data_enough:
            p, anomaly = anomaly_check(data)
            if anomaly:
                # handle
                # send_to_decision_maker(data)
                if is_normal_kpi_type(kpi_name):
                    if long_term_check(data):
                        print('Anomaly Time:{} CMDB:{} KPI:{} Value:{} P-value:{} History:{}'
                              .format(to_string_date(timestamp), cmdb_id, kpi_name, value, p, history_list))
                        send_to_decision_maker(data)
                        # 如果异常则不保存数据
                        continue
        # 检测后，将数据写入内存
        metric_stash(data)


if __name__ == '__main__':
    metric_monitor()
