from copy import deepcopy
import time

import numpy as np

from client import cli_msg


class Msg:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.tag = time.time()

    def __repr__(self):
        ret = ""
        for k in vars(self):
            ret += f'{k}: {getattr(self, k)} '
        return ret


class Reply:
    def __init__(self, res, term):
        self.res = res
        self.term = term


class Replica:
    def __init__(self, args):
        # leader election
        self.cur_term = 0
        self.voted_for = None
        self.state = 'follower'
        self.lst_leader_candidate_T = -args.heartbeat_to
        self.ele_to_T = None
        self.votes_received = None
        self.lst_sent_T = None
        # log
        self.log = [(-1, 0)]  # [(term, val)]
        self.applied_val = []
        self.com_index = 0
        self.lst_apply = 0
        self.nxt_idx = None
        self.matched_idx = None

    def __repr__(self):
        ret = ""
        for k in vars(self):
            ret += f'{k}: {getattr(self, k)} '
        return ret


def replica_fn(name,
               T, T_end,
               rep_to_com_queue, com_to_rep_queue, num_replica,
               seed, LOG,
               args,
               ):
    F = num_replica // 2
    np.random.seed(seed)
    launched = False
    R = Replica(args)

    # lst_log_T = -1

    while T.value < T_end:
        if (T.value > 0) and not launched:
            launched = True
            print(f'Launch replica {name} at time {T.value}')
        if not launched:
            continue

        # if T.value > lst_log_T:
        #     lst_log_T = T.value
        #     print(f'Time {T.value}, replica {name}, state {R}')

        while R.lst_apply < R.com_index and T.value < T_end:
            R.lst_apply += 1
            R.applied_val.append(R.log[R.lst_apply][1])
            if R.state == 'leader':
                rep_to_com_queue.put_nowait(Reply(R.applied_val, R.cur_term))

        if not com_to_rep_queue.empty():
            msg = com_to_rep_queue.get_nowait()
            if isinstance(msg, cli_msg):
                if R.state == 'leader':
                    processed = False
                    for x in R.log:
                        if x[1] == msg.val:
                            processed = True
                            break
                    if not processed:
                        R.log.append((R.cur_term, msg.val))
                        for i in range(num_replica):
                            if i != name and R.nxt_idx[i] < len(R.log):
                                R.lst_sent_T[i] = T.value
                                rep_to_com_queue.put_nowait((T.value, Msg(
                                    to=i, type='req_apd', term=R.cur_term, sender_is_leader=True,
                                    sender=name,
                                    prev_log_idx=R.nxt_idx[i] - 1,
                                    entries=deepcopy(R.log[R.nxt_idx[i] - 1:]),  # first is prev_log_term
                                    leader_cmt=R.com_index,
                                )))
            else:
                # print(f'Time {T.value}, replica {name}:\n\treceive {msg=}\n\tstate {R}')
                if msg.term > R.cur_term:
                    R.cur_term = msg.term
                    R.state = 'follower'
                    R.voted_for = None
                if msg.term >= R.cur_term:
                    if msg.sender_is_leader:
                        R.lst_leader_candidate_T = T.value
                        if R.state == 'candidate':
                            R.state = 'follower'
                    if msg.type == 'req_vote':
                        R.lst_leader_candidate_T = T.value
                        if R.voted_for is None:
                            if msg.lst_log_term > R.log[-1][0] or (msg.lst_log_term == R.log[-1][0] and msg.lst_log_idx >= len(R.log) - 1):
                                R.voted_for = msg.sender
                        if R.voted_for == msg.sender:
                            rep_to_com_queue.put_nowait((T.value, Msg(to=msg.sender, type='rep_vote', term=R.cur_term, sender_is_leader=False)))
                    elif msg.type == 'rep_vote':
                        if R.state == 'candidate':
                            assert msg.term == R.cur_term
                            R.votes_received += 1
                            if R.votes_received == F + 1:
                                print(f'At time {T.value}, replica-{name} elected as leader for term {R.cur_term}.')
                                R.state = 'leader'
                                R.lst_sent_T = [-1 for _ in range(num_replica)]
                                R.nxt_idx = [len(R.log) for _ in range(num_replica)]
                                R.matched_idx = [0 for _ in range(num_replica)]
                    elif msg.type == 'req_apd':
                        assert R.state == 'follower'
                        matched_idx = None
                        if msg.prev_log_idx < len(R.log):
                            assert 0 <= msg.prev_log_idx < len(R.log), f'{name=}, {R.log=}, {msg.prev_log_idx=}'
                            if R.log[msg.prev_log_idx][0] == msg.entries[0][0]:
                                R.log = R.log[:msg.prev_log_idx] + msg.entries
                                matched_idx = len(R.log) - 1

                                new_com_idx = min(msg.leader_cmt, len(R.log) - 1)
                                # assert new_com_idx >= R.com_index, f'time {T.value}, replica {name} attempts to update commit index ({R.com_index}) to a smaller value ({new_com_idx})'
                                # leader's commit-idx can be out-dated when leader repeatedly sends out req_apd and communication is not FIFO
                                R.com_index = max(R.com_index, new_com_idx)
                            else:
                                R.log = R.log[:msg.prev_log_idx]
                        # print(f'\ttime {T.value}, replica {name} put message to {msg.sender}, replying req_apd')
                        rep_to_com_queue.put_nowait((T.value, Msg(
                            to=msg.sender, type='rep_apd', term=R.cur_term, sender_is_leader=False,
                            sender=name, matched_idx=matched_idx, com_index=R.com_index,
                            new_prev_log_idx=len(R.log) - 1,
                        )))
                    elif msg.type == 'rep_apd':
                        assert R.state == 'leader'
                        i = msg.sender
                        if msg.matched_idx is None:
                            R.nxt_idx[i] = msg.new_prev_log_idx + 1
                            R.lst_sent_T[i] = T.value
                            rep_to_com_queue.put_nowait((T.value, Msg(
                                to=i, type='req_apd', term=R.cur_term, sender_is_leader=True,
                                sender=name,
                                prev_log_idx=R.nxt_idx[i] - 1,
                                entries=deepcopy(R.log[R.nxt_idx[i] - 1:]),  # first is prev_log_term
                                leader_cmt=R.com_index,
                            )))
                        else:
                            R.nxt_idx[i] = msg.matched_idx + 1
                            R.matched_idx[i] = msg.matched_idx
                            while R.com_index + 1 < len(R.log) and len([j for j in range(num_replica) if
                                                                        (j == name) or (R.matched_idx[j] >= R.com_index + 1)]) >= F + 1:
                                R.com_index += 1
                                assert R.log[R.com_index][0] <= R.cur_term, f'time {T.value}, replica {name}, state: {R}'

        if (R.state == 'follower') and (T.value - R.lst_leader_candidate_T >= args.heartbeat_to):
            R.state = 'candidate'
            R.ele_to_T = T.value + np.random.uniform(args.ele_to, 2 * args.ele_to)
            R.cur_term += 1
            R.votes_received = 1
            R.voted_for = name
            # print(f'Time {T.value}, replica {name} request vote for term {R.cur_term}')
            for i in range(num_replica):
                if i != name:
                    rep_to_com_queue.put_nowait((T.value, Msg(to=i, type='req_vote', term=R.cur_term, sender_is_leader=False, sender=name, lst_log_idx=len(R.log) - 1, lst_log_term=R.log[-1][0])))
        if (R.state == 'candidate') and (T.value >= R.ele_to_T):
            R.state = 'follower'
        if R.state == 'leader':
            for i in range(num_replica):
                if i != name and T.value > R.lst_sent_T[i] + args.leader_send_to:
                    R.lst_sent_T[i] = T.value
                    rep_to_com_queue.put_nowait((T.value, Msg(
                        to=i, type='req_apd', term=R.cur_term, sender_is_leader=True,
                        sender=name,
                        prev_log_idx=R.nxt_idx[i] - 1,
                        entries=deepcopy(R.log[R.nxt_idx[i] - 1:]),  # first is prev_log_term
                        leader_cmt=R.com_index,
                    )))
                    # rep_to_com_queue.put_nowait((T.value, Msg(to=i, type='heartbeat', term=R.cur_term, sender_is_leader=True,)))
