import os
from time import time

current_time = time()
previous = None
data = {}
for f in os.listdir("analysis"):
    action, round_num, col, task, timestamp = f.split("____")
    timestamp = float(timestamp)
    round_num = int(round_num)

    if round_num not in data:
        data[round_num] = {}

    if col not in data[round_num]:
        data[round_num][col] = {}

    if "start" not in data[round_num][col]:
        data[round_num][col]["start"] = []

    if "end" not in data[round_num][col]:
        data[round_num][col]["end"] = []

    data[round_num][col][action].append([task, timestamp])

last_round = previous if previous is not None else max(data.keys())


some_counts = {}
a = 0
for col in data[last_round]:
    if len(data[last_round][col]["start"]) == 3 and len(data[last_round][col]["end"]) == 3:
        if previous is not None:
            duration = round(current_time - data[last_round][col]["end"][-1][-1])
            duration2 = round(current_time - data[last_round][col]["start"][0][-1])
            print(col)
            print("started since", duration2)
            print("finished since", duration)
        continue
    if previous is not None:
        print(col, "---------------------------------")
        continue
    a += 1
    if len(data[last_round][col]["start"]) == len(data[last_round][col]["end"]):
        duration = round(current_time - data[last_round][col]["end"][-1][-1])
        print(col, "finished", len(data[last_round][col]["start"]), "since", duration)
        key = "finished" + str(len(data[last_round][col]["start"]))
    else:
        duration = round(current_time - data[last_round][col]["start"][-1][-1])
        print(col, "started", len(data[last_round][col]["start"]), "since", duration)
        key = "started" + str(len(data[last_round][col]["start"]))
    some_counts[key] = some_counts.get(key, 0) + 1

if previous is None:
    print("stalling:", a)
    print("round:", last_round)

    print("num process + 2:")
    os.system('ps aux | grep "python collaborator.py" | wc -l')

    print(some_counts)
