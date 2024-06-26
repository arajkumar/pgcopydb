class RedactedException(Exception):
    def __init__(self, err, redacted):
        super().__init__(str(err))
        self.err = err
        self.redacted = redacted