import numpy as np


# 一阶差分
def difference(seq):
    length = len(seq)
    res = []
    for i in range(1, length):
        res.append(seq[i] - seq[i - 1])
    return res


# Min-Max归一化
def normalize(seq):
    max_val = max(seq)
    min_val = min(seq)
    res = []
    # 若序列相同则直接返回
    if max_val == min_val:
        return seq
    # 若稳定序列突变，则取变化值
    # system_a 磁盘异常 即为长期30突变至50，可考虑延迟计算？
    # tcp很多指标也会突变，但并非异常，需结合其他指标考虑
    # 可按照异常网络指标占比判定异常程度
    if max(seq[:-1]) == min(seq[:-1]):
        for val in seq:
            res.append(val - seq[0])
        return res
    for val in seq:
        res.append((val - min_val) / (max_val - min_val))
    return res


# Z-score 归一化
def z_normalize(seq):
    mean = np.mean(seq)
    std = np.std(seq)
    # 若序列相同则直接返回
    if np.fabs(std) < 1e-6:
        return seq
    res = []
    for val in seq:
        res.append((val - mean) / std)
    return res
