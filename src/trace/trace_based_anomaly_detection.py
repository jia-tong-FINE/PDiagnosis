import json
import queue


class TraceModel:
    model_patterns = []
    model = {}
    # key = 'cmdb1,cmdb2'
    fixed_trace_q = queue.Queue(5)
    anomaly_cmdb_id = {}
    anomaly_count = {}
    anomaly_time = ''

    def anomaly_detection_with_queue(self, timestamp):
        anomaly_list = []
        anomaly_cmdb_id_time = {}
        trace_1 = self.fixed_trace_q.get()
        for key in trace_1:

            if (trace_1[key] > 500) and ('docker' not in key):
                anomaly_cmdb_id_time[key] = timestamp

        while not self.fixed_trace_q.empty():
            trace_ = self.fixed_trace_q.get()
            for key in trace_:
                if key.startswith(','):
                    if (trace_[key] < 500) and (key in anomaly_cmdb_id_time.keys()):
                        del anomaly_cmdb_id_time[key]

        # print (anomaly_cmdb_id_time)
        if self.anomaly_time == '':
            self.anomaly_time = timestamp
        for key in anomaly_cmdb_id_time.keys():
            if key.split(',')[1] in self.anomaly_count.keys():
                self.anomaly_count[key.split(',')[1]] += 1
            else:
                self.anomaly_count[key.split(',')[1]] = 1
        main_key = 'null'
        anomaly_time = self.anomaly_time
        # print (int(timestamp.strip())-int(self.anomaly_time.strip()))
        if int(timestamp.strip()) > int(self.anomaly_time.strip()) + 300000:
            # print (self.anomaly_count)
            main_key = ''
            max = 0
            for key in self.anomaly_count.keys():
                if self.anomaly_count[key] > max:
                    max = self.anomaly_count[key]
                    main_key = key
            self.anomaly_count.clear()
            self.anomaly_time = timestamp

        return main_key, anomaly_time

    def update_model(self, trace_point):
        key = trace_point.parent_cmdb_id + ',' + trace_point.cmdb_id
        self.model[key] = trace_point.real_duration

    def update_model_pattern(self, trace_parser):
        self.model.clear()
        for key in trace_parser.complete_trace_path.keys():
            for trace_point in trace_parser.complete_trace_path[key]:
                self.update_model(trace_point)
        # print (self.model)
        new_model_pattern = {}
        old_model_pattern_id = -1
        cmp_flag = 0
        for model_pattern in self.model_patterns:
            cmp_result = self.cmp_model_pattern(self.model, model_pattern)
            # print (cmp_result)
            if cmp_result == 1:
                old_model_pattern_id = self.model_patterns.index(model_pattern)
                for key in model_pattern:
                    if model_pattern[key] == -1:
                        new_model_pattern[key] = self.model[key]
                    elif self.model[key] == -1:
                        new_model_pattern[key] = model_pattern[key]
                    else:
                        if self.model[key] > model_pattern[key]:
                            # new_model_pattern[key] = model_pattern[key] + (self.model[key] - model_pattern[key])*0.1
                            new_model_pattern[key] = self.model[key]
                        else:
                            new_model_pattern[key] = model_pattern[key]
                cmp_flag = 1
                break
        if cmp_flag == 1:
            del (self.model_patterns[old_model_pattern_id])
            self.model_patterns.append(new_model_pattern)
        if cmp_flag == 0:
            self.model_patterns.append(self.model)

    def update_model_for_detection(self, trace_parser):
        self.model.clear()
        for key in trace_parser.complete_trace_path.keys():
            for trace_point in trace_parser.complete_trace_path[key]:
                self.update_model(trace_point)

    def cmp_model_pattern(self, model1, model2):
        if len(model1.keys()) != len(model2.keys()):
            return 0
        if len(set(model1.keys()).difference(set(model2.keys()))) == 0:
            return 1
        else:
            return 0

    def model_to_file(self, filepath):
        json.dump(self.model_patterns, open(filepath, 'w'))

    def read_model(self, filepath):
        model_patterns = json.load(open(filepath))
        self.model_patterns = model_patterns

    def add_to_q(self):
        self.fixed_trace_q.put(self.model)

    def anomaly_detection(self):
        anomaly_list = []
        for model_pattern in self.model_patterns:
            cmp_result = self.cmp_model_pattern(self.model, model_pattern)
            # print(cmp_result)
            if cmp_result == 1:
                for key in self.model.keys():
                    if self.model[key] > model_pattern[key] * 10:
                        anomaly_cmdb_id = key.split(',')[1]
                        if 'docker' in anomaly_cmdb_id:
                            continue
                        else:
                            print(key + ':')
                            print(self.model[key])
                            print(model_pattern[key])
                            anomaly_list.append(anomaly_cmdb_id)
            # else:
            #     max = 0
            #     anomaly_key = ''
            #     for key in self.model.keys():
            #         if self.model[key] > max:
            #             max = self.model[key]
            #             anomaly_key = key
            #     # anomaly_list.append(anomaly_key.split(',')[1])
            #
            #     if anomaly_key == '':
            #         continue
            #     # print (anomaly_key.split(',')[1])
            #     if anomaly_key.split(',')[1] not in anomaly_list:
            #         anomaly_list.append(anomaly_key.split(',')[1])
        return anomaly_list
