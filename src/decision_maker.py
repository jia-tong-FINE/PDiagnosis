import json
import logging
import sys
import threading
import time
from collections import Counter

import requests
from flask import Flask
from flask import request

from log.log import log_based_anomaly_detection_entrance
from metrics.metrics import metric_monitor
from trace import trace_entrance

app = Flask(__name__)
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

metric_cache = []
log_cache = []
trace_cache = None
# 上次提交时间戳
last_submission = 0
# 总提交次数
submission = 0
config = {}


def to_string_date(now_stamp):
    t = time.localtime(now_stamp)
    return time.strftime("%Y-%m-%d %H:%M:%S", t)


# 生成候选日志答案
def generate_ans_log(now, cmdb):
    res = set()
    for log in log_cache:
        if log['timestamp'] > now or now - log['timestamp'] < config['submit_anomaly_threshold']:
            if log['cmdb_id'] == cmdb:
                res.add(log['logname'])
    log_now = log_cache[:]
    # 去除过期日志
    for k, v in log_now:
        if now - v['timestamp'] >= config['submit_anomaly_threshold']:
            del log_cache[k]
    return res


# 从候选生成答案指标
# load -> load, cpu
# net -> net, tcp
# disk -> free pct used
def generate_ans_kpi(candidate_kpi):
    res = set()
    global config
    keywords = list(config['kpi_group'].keys())
    avail_keyword = set()
    for candidate in candidate_kpi:
        for keyword in keywords:
            if keyword in candidate:
                avail_keyword.add(keyword)
    for keyword in avail_keyword:
        for name in config['kpi_group'][keyword]:
            res.add(name)
    # Dropout exceeded kpis
    while len(res) > config['submission_limit']:
        res.pop()
    return res


# 提交答案
def commit_answer(ans_cmdb, ans_kpi):
    answer = [[ans_cmdb, k] for k in ans_kpi]
    # submit by official method
    submit(answer)


# 官方提供的submit
def submit(ctx):
    global config
    assert (isinstance(ctx, list))
    for tp in ctx:
        assert (isinstance(tp, list))
        assert (len(tp) == 2)
        assert (isinstance(tp[0], str))
        assert (isinstance(tp[1], str) or (tp[1] is None))
    data = {'content': json.dumps(ctx)}
    r = requests.post(config['commit_address'], data=json.dumps(data))
    return r.text


# 决定是否提交
def check_submission(data):
    global metric_cache
    global last_submission
    global submission
    global config
    # 如果当前缓存为空，则不提交
    if len(metric_cache) == 0:
        metric_cache.append(data)
        return False
    last_anomaly = metric_cache[-1]['timestamp']
    timestamp = data['timestamp']
    # 如果当前异常数据过期，则放弃并不提交
    if timestamp < last_submission or timestamp < last_anomaly:
        return False
    # 如果当前异常数据距离上个异常点多于y分钟，则清空缓存（认为当前缓存了噪声，即异常数据必然连续两分钟以上）
    if timestamp - last_anomaly > config['submit_span_threshold']:
        metric_cache.clear()
        metric_cache.append(data)
        return False
    # 提交间隔至少k分钟
    if timestamp - last_submission < config['submit_anomaly_threshold']:
        # 是否保存？
        return False
    # 缓存的异常总数
    anomaly_count = len(metric_cache)
    # 缓存内的异常总数多于阈值才开始提交
    if anomaly_count < config['anomaly_num_threshold']:
        metric_cache.append(data)
        return False
    # 持续时间
    anomaly_time_span = metric_cache[-1]['timestamp'] - metric_cache[0]['timestamp']
    # 持续时间长于阈值才开始提交，设置为60则表明至少采集2分钟数据,120表示3分钟，等等
    if anomaly_time_span < config['submit_span_threshold']:
        metric_cache.append(data)
        return False
    # 异常cmdb
    anomaly_cmdbs = [d['cmdb_id'] for d in metric_cache]
    # 异常kpi
    anomaly_kpis = [d['kpi_name'] for d in metric_cache]
    # 异常时间戳
    anomaly_times = [d['timestamp'] for d in metric_cache]
    cmdb_count = Counter(anomaly_cmdbs)
    kpi_count = Counter(anomaly_kpis)
    ans_count = cmdb_count.most_common(1)[0][1]
    ans_cmdb = cmdb_count.most_common(1)[0][0]
    candidate_kpi = []
    for anomaly in metric_cache:
        if anomaly['cmdb_id'] == ans_cmdb:
            candidate_kpi.append(anomaly['kpi_name'])
    ans_log = generate_ans_log(timestamp, ans_cmdb)
    ans_kpi = generate_ans_kpi(candidate_kpi)
    # 如果kpi为空集则说明异常指标不在已总结的根因之内
    # if not ans_kpi:
    #     metric_cache.clear()
    #     metric_cache.append(data)
    #     return False
    commit_answer(ans_cmdb, ans_kpi | ans_log)
    last_submission = timestamp
    print(log_cache)
    print(
        '[COMMIT]: {}, {} commit {}, KPI:{}'.format(to_string_date(timestamp), str(submission), ans_cmdb, str(ans_kpi)))
    metric_cache.clear()
    return True


@app.route('/')
def hello_world():
    return 'Hello World'


@app.route('/trace', methods=['POST'])  # http://127.0.0.1:5000/trace
def trace_handle():
    a = request.get_data()
    data = json.loads(a)
    return 'success'


@app.route('/metric', methods=['POST'])
def metric_handle():
    global submission
    metric = request.get_data()
    metric = json.loads(metric)
    if check_submission(metric):
        submission = submission + 1
    return 'success'


@app.route('/log', methods=['POST'])
def log_handle():
    a = request.get_data()
    data = json.loads(a)
    log_cache.append(data)
    print('[LOG] Received:' + data)
    input()
    return 'success'


def monitor_m():
    while True:
        nowThreadsName = []
        now = threading.enumerate()
        for i in now:
            nowThreadsName.append(i.getName())
        if 'metric_thread' in nowThreadsName:
            print(time.strftime("%Y-%m-%d %H:%M:%S ", time.localtime()) + 'metric thread alive')
            pass
        else:
            print('stopped，now restart')
            t = threading.Thread(target=metric_monitor, args=(config,))  # 重启线程
            t.setName('metric_thread')  # 重设name
            t.start()
        time.sleep(180)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('please specify config file')
        exit()
    print(sys.argv[1])
    with open(sys.argv[1]) as f:
        config = json.load(f)
        print(config)
        threadingDict = {}
        thread_m = threading.Thread(target=metric_monitor, args=(config,))
        thread_m.setName('metric_thread')
        thread_m.start()
        thread_l = threading.Thread(target=log_based_anomaly_detection_entrance,
                                    args=(config,))
        thread_l.start()
        if config['use_trace']:
            thread_t = threading.Thread(target=trace_entrance.trace_based_anomaly_detection_entrance, args=(config,))
            thread_t.start()
        thread_monitor = threading.Thread(target=monitor_m)
        thread_monitor.start()
        app.run(port=config['decision_port'])
