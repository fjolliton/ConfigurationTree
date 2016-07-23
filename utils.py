from typing import Optional, Callable


class Printer:

    __slots__ = ['__prefix', '__output']

    def __init__(self, prefix: str='', output: Callable=print) -> None:
        self.__prefix = prefix
        self.__output = output

    def __call__(self, line: str, misc: Optional[str]=None) -> None:
        r = self.__prefix + line
        if misc:
            r = '{:<20}{}'.format(r, misc)
        self.__output(r)

    def shift(self, amount: int) -> 'Printer':
        return Printer(self.__prefix + ' ' * amount, self.__output)

