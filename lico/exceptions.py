class LicoError(Exception):
    pass


class RowProcessError(LicoError):
    pass


class MissingInputColumn(RowProcessError):
    pass
