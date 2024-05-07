import json
import time
from pathlib import Path
from datetime import datetime

import numpy as np


class cli_msg:
    def __init__(self, val):
        self.val = val


def client_fn(name,
              T, T_end,
              cli_in_queue, com_in_queues,
              req_freq, seed,
              warm_up, out
              ):
    out_dir = Path(__file__).parent.joinpath(out)
    out_dir.mkdir(exist_ok=True, parents=True)
    out_f = out_dir.joinpath(f'{datetime.now().strftime("%m_%d-%H_%M_%S.json")}')

    CNT = 0
    gt = []
    latencies = []
    term_change_latencies = []
    lst_to = 0
    lst_term = -1

    launched = False
    while T.value < T_end:
        if (T.value > warm_up) and not launched:
            launched = True
            print(f'Launch client {name} at time {T.value}')
        if not launched:
            continue

        CNT += 1
        gt.append(CNT)
        t_beg = T.value
        lst_send_t = t_beg
        for com_in_queue in com_in_queues:
            com_in_queue.put_nowait((t_beg, cli_msg(CNT)))

        while T.value < T_end:
            if not cli_in_queue.empty():
                res, info = cli_in_queue.get_nowait()
                t_end = T.value
                if res[-1] == CNT:
                    break
            if T.value > lst_send_t + req_freq:
                lst_send_t = T.value
                for com_in_queue in com_in_queues:
                    com_in_queue.put_nowait((t_beg, cli_msg(CNT)))
        if T.value > T_end:
            lst_to = T.value - t_beg
            break

        if res != gt:
            print(f'Time {t_end}, client receives wrong result:\n\texpected: {gt}\n\treceived: {res}')
            BEG = time.time()
            while time.time() < BEG + 1:
                T.value = T_end + 1
            with out_f.open('w') as F:
                json.dump(dict(correct=False), F)
            return
        # print(f'Time {t_end}, client receives {res}')
        latencies.append(t_end - t_beg)

        term = info['term']
        if term != lst_term:
            if lst_term != -1:
                term_change_latencies.append(t_end - t_beg)
            lst_term = term

    time.sleep(0.1)
    print('='*40+"SUMMARY"+"="*40)
    print(f'Correct!')
    print(f'avg latency: {np.mean(latencies)}')
    print(f'term change latency: {np.mean(term_change_latencies)}')
    print(f'last message wait time: {lst_to}')
    with out_f.open('w') as F:
        json.dump(dict(correct=True, avg_latency=np.mean(latencies), term_change_latency=np.mean(term_change_latencies), last_wait_time=lst_to), F)
