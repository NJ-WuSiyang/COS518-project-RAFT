import numpy as np

from heap import TimeOrderedHeap
from network import get_status
from replica import Reply


def communicator_fn(name,
                    T, T_end,
                    G,
                    com_in_queues,  # communicator in_queues, used to receive append message to a communicator
                    rep_to_com_queue, com_to_rep_queue,
                    cli_in_queue,
                    seed,
                    ):
    np.random.seed(seed)
    heap = TimeOrderedHeap()
    launched = False
    lst_t = [-1 for _ in range(len(com_in_queues))]
    while T.value < T_end:
        if (T.value > 0) and not launched:
            launched = True
            print(f'Launch communicator {name} at time {T.value}')
        if not launched:
            continue

        if not com_in_queues[name].empty():
            msg = com_in_queues[name].get_nowait()  # (t, val)
            assert isinstance(msg, tuple) and (not isinstance(msg[1], tuple))
            heap.push(msg[0], msg[1])

        if not rep_to_com_queue.empty():
            msg = rep_to_com_queue.get_nowait()  # (t, val)
            if isinstance(msg, Reply):
                cli_in_queue.put_nowait((msg.res, dict(term=msg.term)))
            else:
                assert isinstance(msg, tuple) and (not isinstance(msg[1], tuple))
                t, val = msg
                # print(f'Time {t}, replica {name} sends: {val}')
                assert t < T_end, f'Replica cannot send a message of starting time {t} larger than {T_end}!'
                if name == val.to:
                    # print(f'Time={t}, replica {name} send message: {val}, arrive at {t}')
                    assert not isinstance(val, tuple)
                    com_to_rep_queue.put_nowait(val)
                else:
                    net_status = get_status(G[name][val.to], t)
                    if not net_status.fail:
                        if np.random.rand() > net_status.drop:
                            t_rec = t + max(0., np.random.normal(net_status.mu, net_status.std))
                            t_rec = max(t_rec, lst_t[val.to])
                            lst_t[val.to] = t_rec
                            # print(f'Time={t}, replica {name} send message: {val}, arrive at {t_rec}')
                            com_in_queues[val.to].put_nowait((t_rec, val))
#                         else:
#                             print(f'Time={t}, replica {name} send message: {val}, dropped')
#                     else:
#                         print(f'Time={t}, replica {name} send message: {val}, failed')

        if heap.size() > 0:
            t, val = heap.top()
            if t <= T.value:
                heap.pop()
                assert not isinstance(val, tuple)
                com_to_rep_queue.put_nowait(val)
