import os
import time
import sys

current_time = time.time()

# filters = [45, 30, 35, 48, 23, 10]
filters = list(range(1, 51))
filters = [f"col{ff}@example.com" for ff in filters]


def summary():
    cols = {}
    for file in os.listdir("analysis2"):
        line = file.strip().split()
        if line[1] == "SendLocalTaskResults":
            assert len(line) == 6
            colname, method, round_number, timestamp, action, _ = line
            if colname not in cols:
                cols[colname] = []
            cols[colname].append([method, round_number, action, current_time - float(timestamp)])
        else:
            assert len(line) == 7
            colname, method, tensor_name, round_number, timestamp, action, _ = line
            if colname not in cols:
                cols[colname] = []
            cols[colname].append([method, tensor_name, round_number, action, current_time - float(timestamp)])

    for col in filters:
        cols[col].sort(key=lambda x: -x[-1])
        print(col, cols[col][-1])


def for_col(colname):
    colname = f"col{colname}@example.com"
    col = []
    for file in os.listdir("analysis2"):
        line = file.strip().split()
        if line[1] == "SendLocalTaskResults":
            if line[0] != colname:
                continue
            assert len(line) == 6
            _, method, round_number, timestamp, action, _ = line
            col.append([method, round_number, action, current_time - float(timestamp)])
        else:
            if line[0] != colname:
                continue
            assert len(line) == 7
            _, method, tensor_name, round_number, timestamp, action, _ = line
            col.append([method, tensor_name, round_number, action, current_time - float(timestamp)])

    col.sort(key=lambda x: -x[-1])
    print(*col, sep="\n")
    print(len(col))


if len(sys.argv) > 1:
    colname = sys.argv[1]
    for_col(int(colname))
else:
    summary()
