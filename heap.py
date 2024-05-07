class TimeOrderedHeap:
    def __init__(self):
        self._buf = []

    def push(self, t, val):
        idx = len(self._buf)
        self._buf.append((t, val))
        while idx > 0:
            par = (idx - 1) // 2
            if self._buf[idx][0] < self._buf[par][0]:
                self._buf[idx], self._buf[par] = self._buf[par], self._buf[idx]
            else:
                break
            idx = par

    def size(self):
        return len(self._buf)

    def top(self):
        return self._buf[0]

    def pop(self):
        self._buf[0] = self._buf[len(self._buf) - 1]
        self._buf = self._buf[:-1]
        idx = 0
        while idx < len(self._buf):
            ch = idx * 2 + 1
            if ch >= len(self._buf):
                break
            if ch + 1 < len(self._buf):
                if self._buf[ch][0] > self._buf[ch + 1][0]:
                    ch = ch + 1
            if self._buf[idx][0] > self._buf[ch][0]:
                self._buf[idx], self._buf[ch] = self._buf[ch], self._buf[idx]
                idx = ch
            else:
                break
