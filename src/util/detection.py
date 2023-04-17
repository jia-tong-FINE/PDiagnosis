import numpy as np
from sklearn.neighbors import KernelDensity

from . import preprocess


# 核密度估计检验
def kde_detect(history, now):
    diff_history = history[:]
    diff_history.append(now)
    # 取差分
    # diff_history = preprocess.difference(diff_history)
    # 归一化
    diff_history = preprocess.normalize(diff_history)
    now = diff_history[-1]
    del diff_history[-1]
    diff_history = np.array(diff_history)
    diff_history = np.reshape(diff_history, (-1, 1))
    esti = KernelDensity().fit(diff_history)
    return np.exp(esti.score_samples([[now]]))


# 3-Sigma 检验
def sigma_detect(history, now):
    mean = np.mean(history)
    std = np.std(history)
    return now > mean + 3 * std or now < mean - 3 * std


# 历史阈值机制检验
def max_min_detect(history, now):
    max_h = max(history)
    min_h = min(history)
    return now > max_h or now < min_h


# 历史均值检验
def history_mean_detect(history, now):
    mean = np.mean(history)
    # 变化需多于历史均值的5%
    return now > 1.05 * mean or now < 0.95 * mean


# 微小扰动检验
def error_detect(history, now):
    return np.fabs(now - history[-1]) > 0.5
