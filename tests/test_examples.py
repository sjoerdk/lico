from lico.core import Operation
from lico.exceptions import RowProcessError


class MyOperation(Operation):
    def apply(self, row):
        if row['col1'] == '0':
            # This value is bad for some reason. Cannot process
            raise RowProcessError
        return row

    def has_previous_result(self, row):
        """This gets called on any row before processing. If it
          returns True the line will be skipped
          """
        if row.get('result', None):
            return True
        else:
            return False


