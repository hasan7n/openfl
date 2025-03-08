import time
import os

current_time = time.time()
cols = {}
streams = []
for file in os.listdir("analysis2"):
    line = file.strip().split()
    if line[1] == "STREAM":
        assert len(line) == 5
        call_id, _, timestamp, action, _ = line
        timestamp = current_time - float(timestamp)
        streams.append([call_id, action, float(timestamp)])
    elif line[1] == "SendLocalTaskResults":
        assert len(line) == 6
        colname, method, round_number, timestamp, action, _ = line
        timestamp = current_time - float(timestamp)
        if colname not in cols:
            cols[colname] = []
        cols[colname].append([method, round_number, action, float(timestamp)])
        if method == "SendLocalTaskResults":
            cols[colname][-1].pop(1)
    else:
        assert len(line) == 7
        colname, method, tensor_name_or_call_id, round_number, timestamp, action, _ = line
        timestamp = current_time - float(timestamp)
        if colname not in cols:
            cols[colname] = []
        cols[colname].append([method, tensor_name_or_call_id, round_number, action, float(timestamp)])
        if method == "SendLocalTaskResults":
            cols[colname][-1].pop(1)

for col in cols:
    cols[col].sort(key=lambda x: -x[-1])
    print(col, cols[col][-1])

streams.sort(key=lambda x: -x[-1])

hanging = {}
for call_id, action, timestamp in streams:
    if action == "START":
        assert call_id not in hanging
        hanging[call_id] = timestamp
    else:
        assert call_id in hanging
        del hanging[call_id]

print("hanging streams:", hanging)
print(len(hanging))
