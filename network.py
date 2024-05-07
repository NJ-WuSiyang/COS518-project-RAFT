import numpy as np


class NodeStatus:
    def __init__(self, t0, t1, fail):
        self.t0 = t0
        self.t1 = t1
        self.fail = fail

    def __repr__(self):
        ret = ""
        for k in vars(self):
            ret += f'{k}: {getattr(self, k)} '
        return ret


class Edge:
    def __init__(self, t0, t1, fail, **kwargs):
        self.t0 = t0
        self.t1 = t1
        self.fail = fail
        if not fail:
            self.mu = kwargs['mu']
            self.std = kwargs['std']
            self.drop = kwargs['drop']

    def __repr__(self):
        ret = ""
        for k in vars(self):
            ret += f'{k}: {getattr(self, k)} '
        return ret


def get_status(Gij, t, return_idx=False):
    l, r = 0, len(Gij)
    while l < r:
        mid = (l + r) // 2
        if Gij[mid].t0 <= t < Gij[mid].t1:
            if return_idx:
                return Gij[mid], mid
            return Gij[mid]
        elif Gij[mid].t0 > t:
            r = mid
        else:
            l = mid + 1
    raise RuntimeError(f'Cannot find status at time {t} for {Gij=}')


def solve_network_graph(args):
    num_replica = args.num_replica
    F = num_replica // 2
    T_end = args.T_end
    G = [[[] for j in range(num_replica)] for i in range(num_replica)]

    # generate geographic location
    # geo_loc = np.random.rand(num_replica, 2)  # (n, 2)
    # loc_dist = np.linalg.norm(geo_loc[:, None, :] - geo_loc[None, :, :], axis=-1)  # (n, n)
    # delay_mu = loc_dist * args.delay_dist_ratio  # (n, n)
    #
    # print(f'Network delay:\n{delay_mu}')
    #
    # delay_std = np.random.rand(num_replica, num_replica) * (args.delay_std_ub - args.delay_std_lb) + args.delay_std_lb  # (n, n)
    delay_info = np.load(f'delay_{num_replica}.npz')
    delay_mu = delay_info['delay_mu']
    delay_std = delay_info['delay_std']
    print(f'delay_mu:\n{delay_mu}\ndelay_std:\n{delay_std}')
    # drop = np.random.rand(num_replica, num_replica) * (args.drop_ub - args.drop_lb) + args.drop_lb  # (n, n)
    drop = np.zeros_like(delay_mu)
    E_kwargs = [[dict(mu=delay_mu[i][j], std=delay_std[i][j], drop=drop[i][j]) for j in range(num_replica)] for i in range(num_replica)]

    if args.failure_type == 'node':
        nodes_status = []
        for i in range(num_replica):
            status = []
            T = 0
            while T < T_end:
                if np.random.rand() < args.failure_prob:
                    T_prev = 0 if len(status) == 0 else status[-1].t1
                    status.append(NodeStatus(T_prev, T, fail=False))
                    T_nxt = min(T + np.random.normal(args.failure_time_mu, args.failure_time_std), T_end)
                    status.append(NodeStatus(T, T_nxt, fail=True))
                    T = T_nxt
                else:
                    T += args.tstep
            T_prev = 0 if len(status) == 0 else status[-1].t1
            if T_prev < T_end:
                status.append(NodeStatus(T_prev, T_end, fail=False))
            nodes_status.append(status)

        while True:
            unq_ts = []
            for i in range(num_replica):
                for x in nodes_status[i]:
                    unq_ts.append(x.t0)
            unq_ts = np.unique(unq_ts)

            all_succ = True
            for t in unq_ts:
                if t == T_end:
                    continue

                fail_ik = []
                for i in range(num_replica):
                    status, idx = get_status(nodes_status[i], t, return_idx=True)
                    if status.fail:
                        fail_ik.append((i, idx))

                if len(fail_ik) <= F:
                    continue

                all_succ = False
                i, k = fail_ik[np.random.randint(len(fail_ik))]
                nodes_status[i][k].fail = False
            if all_succ:
                break

        # debug
        # print('nodes status')
        # for node_status in nodes_status:
        #     print('\t', node_status)

        for i in range(num_replica):
            for j in range(i):
                ti, tj, T = 0, 0, 0
                while T < T_end:
                    T_nxt = min(nodes_status[i][ti].t1, nodes_status[j][tj].t1)
                    G[i][j].append(Edge(T, T_nxt, fail=nodes_status[i][ti].fail or nodes_status[j][tj].fail, **E_kwargs[i][j]))
                    T = T_nxt
                    if T >= nodes_status[i][ti].t1:
                        ti += 1
                    if T >= nodes_status[j][tj].t1:
                        tj += 1
                G[j][i] = G[i][j]
    elif args.failure_type in ['partition', 'partition-F+1', 'partition-diff-F+1']:
        T, T_prev = 0, 0
        lst_avail = None
        while T < T_end:
            if np.random.rand() < args.failure_prob:
                for i in range(num_replica):
                    for j in range(i):
                        G[i][j].append(Edge(T_prev, T, fail=False, **E_kwargs[i][j]))
                T_nxt = min(T + np.random.normal(args.failure_time_mu, args.failure_time_std), T_end)

                if args.failure_type == 'partition':
                    num_avail = np.random.randint(F + 1, num_replica + 1)
                    avail = np.zeros(num_replica, dtype=np.bool_)
                    avail[np.random.choice(num_replica, num_avail, replace=False)] = True
                elif args.failure_type == 'partition-F+1':
                    num_avail = F + 1
                    avail = np.zeros(num_replica, dtype=np.bool_)
                    avail[np.random.choice(num_replica, num_avail, replace=False)] = True
                elif args.failure_type == 'partition-diff-F+1':
                    if lst_avail is None:
                        num_avail = F + 1
                        avail = np.zeros(num_replica, dtype=np.bool_)
                        avail[np.random.choice(num_replica, num_avail, replace=False)] = True
                        lst_avail = avail.copy()
                    else:
                        idx_not_to_flip = np.nonzero(lst_avail)[0][np.random.randint(F + 1)]
                        avail = ~lst_avail
                        avail[idx_not_to_flip] = lst_avail[idx_not_to_flip]
                else:
                    raise NotImplementedError

                for i in range(num_replica):
                    for j in range(i):
                        G[i][j].append(Edge(T, T_nxt, fail=avail[i] != avail[j], **E_kwargs[i][j]))

                T = T_nxt
                T_prev = T_nxt
            else:
                T += args.tstep
        if T_prev < T_end:
            for i in range(num_replica):
                for j in range(i):
                    G[i][j].append(Edge(T_prev, T_end, fail=False, **E_kwargs[i][j]))
        for i in range(num_replica):
            for j in range(i):
                G[j][i] = G[i][j]
    else:
        raise NotImplementedError

    return G


if __name__ == '__main__':
    delay_dist_ratio = 0.2
    delay_std_lb = 0.01
    delay_std_ub = 0.03
    for num_replica in [3, 5, 7]:
        geo_loc = np.random.rand(num_replica, 2)  # (n, 2)
        loc_dist = np.linalg.norm(geo_loc[:, None, :] - geo_loc[None, :, :], axis=-1)  # (n, n)
        delay_mu = loc_dist * delay_dist_ratio  # (n, n)
        delay_std = np.random.rand(num_replica, num_replica) * (delay_std_ub - delay_std_lb) + delay_std_lb  # (n, n)
        np.savez(f'delay_{num_replica}.npz', geo_loc=geo_loc, delay_mu=delay_mu, delay_std=delay_std)
