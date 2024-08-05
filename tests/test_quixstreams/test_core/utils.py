class Sink(list):
    def append_record(self, value, key, timestamp, headers):
        return self.append((value, key, timestamp, headers))
