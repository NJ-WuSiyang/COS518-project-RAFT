import multiprocessing
from multiprocessing import Process, Value, Array
import argparse
import time
import numpy as np

from client import client_fn
from communicator import communicator_fn
from replica import replica_fn
from network import solve_network_graph


def log_queue_status(all_qs, freq, T, T_end):
    lst_T = 0
    while T.value < T_end:
        if T.value - lst_T > freq:
            lst_T = T.value
            info = f'at time {T.value}, queue_info:'
            for k, v in all_qs:
                info += f'\n\t{k}: {v.qsize()}'
            print(info)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-replica', '-n', type=int, help="#replica", default=5)
    parser.add_argument('--launch-process-time', type=float, default=.5)
    parser.add_argument('--T-end', type=float, default=60.)
    parser.add_argument('--delay-dist-ratio', type=float, default=0.15)
    parser.add_argument('--delay-std-lb', type=float, default=0.01)
    parser.add_argument('--delay-std-ub', type=float, default=0.03)
    parser.add_argument('--drop-lb', type=float, default=0.)
    parser.add_argument('--drop-ub', type=float, default=0.)
    parser.add_argument('--failure-type', '-FT', type=str, default="partition-F+1")
    parser.add_argument('--failure-time-mu', '-FM', type=float, default=1.)
    parser.add_argument('--failure-time-std', '-FS', type=float, default=.3)
    parser.add_argument('--failure-prob', '-FP', type=float, default=.4)
    parser.add_argument('--tstep', type=float, default=.01, help="timestep to simulate network failure")
    parser.add_argument('--req-freq', type=float, default=0.01)
    parser.add_argument('--q-status-tstep', type=float, default=1., help="timestep to simulate network failure")
    parser.add_argument('--mp-method', type=str, default='spawn')
    parser.add_argument('--heartbeat_to', type=float, default=0.3, help='heart beat time-out')
    parser.add_argument('--ele_to', type=float, default=1.0, help='election time-out')
    parser.add_argument('--leader_send_to', type=float, default=0.02)
    parser.add_argument('--warm-up', type=float, default=4.0, help='warm up phase, takes a while to elect first leader')
    parser.add_argument('--out', type=str, default='output')
    args = parser.parse_args()

    multiprocessing.set_start_method(args.mp_method)
    mp_manager = multiprocessing.Manager()
    num_replica = args.num_replica
    T_end = args.T_end
    print(f'{args=}')

    G = solve_network_graph(args)
    print(f'network solved:')
    for i in range(num_replica):
        for j in range(num_replica):
            log = f'G[{i}][{j}] = [\n'
            for x in G[i][j]:
                log += f'\t{x},\n'
            log += ']'
            print(log)

    T = Value('d', -1.)
    LOG = Array('i', [0 for _ in range(num_replica)])
    cli_in_queue = mp_manager.Queue()
    com_in_queues = [mp_manager.Queue() for _ in range(num_replica)]
    all_qs = [('cli_in_queue', cli_in_queue)] + [(f'com_in_queue_{i}', com_in_queues[i]) for i in range(len(com_in_queues))]
    communicator_ps, replica_ps = [], []
    for i in range(num_replica):
        rep_to_com_queue, com_to_rep_queue = mp_manager.Queue(), mp_manager.Queue()
        communicator_ps.append(Process(target=communicator_fn, args=(i, T, T_end, G, com_in_queues, rep_to_com_queue, com_to_rep_queue, cli_in_queue, np.random.randint(10000000))))
        replica_ps.append(Process(target=replica_fn, args=(i, T, T_end, rep_to_com_queue, com_to_rep_queue, num_replica, np.random.randint(10000000), LOG, args)))
        all_qs += [(f'rep_to_com_queue_{i}', rep_to_com_queue), (f'com_to_rep_queue_{i}', com_to_rep_queue)]
    client_p = Process(target=client_fn, args=('client', T, T_end, cli_in_queue, com_in_queues,
                                               args.req_freq, np.random.randint(10000000), args.warm_up, args.out))
    log_q_p = Process(target=log_queue_status, args=(all_qs, args.q_status_tstep, T, T_end))

    client_p.start()
    for communicator_p in communicator_ps:
        communicator_p.start()
    for replica_p in replica_ps:
        replica_p.start()
    log_q_p.start()

    time.sleep(args.launch_process_time)
    WALLCLOCK_BEG = time.time()
    while T.value < T_end:
        T.value = time.time() - WALLCLOCK_BEG
    T.value = T_end + 1

    print(f'Finish at time {T.value}')
    client_p.join()
    for communicator_p in communicator_ps:
        communicator_p.join()
    for replica_p in replica_ps:
        replica_p.join()
    log_q_p.join()


if __name__ == '__main__':
    main()
