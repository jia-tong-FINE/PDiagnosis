# PDiagnose

A multi-source data based anomaly detection and root cause localization tool for microservices.

This repo is based on the code submitted to AIOps Challenge 2021 by Team *Elise Starseeker* from FineLab, Peking University.

## Dependencies

- Python 3.9
- numpy==1.21
- scipy>=1.7
- sklearn>=1.0
- kafka-python>=2.0
- flask>=2.0
- requests>=2.26

## Usage
First set up the kafka server according to the [AIOps Challenge 2021 Description](http://iops.ai), then execute the start shell script.

If anything unexpected happens, you are welcomed to raise an issue.

## Reference

If you want to use PDiagnose on your own purpose, please cite the [following paper](http://www.cloud-conf.net/ispa2021/proc/pdfs/ISPA-BDCloud-SocialCom-SustainCom2021-3mkuIWCJVSdKJpBYM7KEKW/264600a493/264600a493.pdf):

- Chuanjia Hou, Tong Jia, Yifan Wu, Ying Li and Jing Han, Diagnosing Performance Issues in Microservices with Heterogeneous Data Source, In the 19th IEEE International Symposium on Parallel and Distributed Processing with Applications (IEEE ISPA), 2021