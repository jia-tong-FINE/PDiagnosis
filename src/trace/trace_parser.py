class TracePoint:
    parent_id = ''
    span_id = ''
    trace_id = ''
    parent_cmdb_id = ''
    cmdb_id = ''
    timestamp = ''
    duration = ''
    real_duration = -1
    child_duration = []

    def __init__(self, parent_id, span_id, trace_id, cmdb_id, timestamp, duration):
        self.parent_id = parent_id
        self.span_id = span_id
        self.trace_id = trace_id
        self.cmdb_id = cmdb_id
        self.timestamp = timestamp
        self.duration = duration

    def add_child_duration(self, duration):
        self.child_duration.append(duration)

    def cal_real_duration(self):
        duration = int(self.duration)
        for child_dur in self.child_duration:
            duration = duration - int(child_dur)
        self.real_duration = duration


class TraceParser:
    complete_trace_path = {}
    incomplete_trace_path = {}

    def cal_duration(self):
        for key in self.complete_trace_path.keys():
            trace_points = self.complete_trace_path[key]
            new_trace_points = []
            for trace_point in trace_points:
                trace_point_id = trace_point.span_id
                for trace_point_2 in trace_points:
                    if trace_point.parent_id == trace_point_2.span_id:
                        trace_point.parent_cmdb_id = trace_point_2.cmdb_id
                    duration_add = 0
                    if trace_point_id == trace_point_2.parent_id and trace_point_id != trace_point_2.span_id:
                        trace_point.child_duration.append(trace_point_2.duration)
                        duration_add = duration_add + int(trace_point_2.duration)
                trace_point.real_duration = int(trace_point.duration) - duration_add
                new_trace_points.append(trace_point)
            self.complete_trace_path[key] = new_trace_points

    # 1: complete one trace; 0: incomplete
    def update_trace_info(self, data):

        if data["trace_id"] in self.incomplete_trace_path.keys():
            trace_point = TracePoint(data["parent_id"], data["span_id"], data["trace_id"], data["cmdb_id"],
                                     data["timestamp"], data["duration"])
            self.incomplete_trace_path[data["trace_id"]].append(trace_point)
            return 0
        else:

            for key in self.incomplete_trace_path.keys():
                self.complete_trace_path[key] = self.incomplete_trace_path[key]
            self.incomplete_trace_path.clear()

            trace_point = TracePoint(data["parent_id"], data["span_id"], data["trace_id"], data["cmdb_id"],
                                     data["timestamp"], data["duration"])
            self.incomplete_trace_path[data["trace_id"]] = [trace_point]

            self.cal_duration()
            return 1
