class LicoError(Exception):
    pass


class RowProcessError(LicoError):
    pass


class MissingInputColumnError(RowProcessError):
    pass
