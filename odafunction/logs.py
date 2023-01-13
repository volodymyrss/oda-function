import re

import logging
import logging_tree

import rdflib
import pathlib

from . import Function, Executor

class ODAFunctionFormatter(logging.Formatter):    
    def __init__(self) -> None:
        super().__init__("%(asctime)s %(loglevelcolor)s%(levelname)7.7s\033[0m \033[33m%(name)20.20s\033[0m %(message)s")

    def repr_arg(self, arg):
        if isinstance(arg, Function):
            return "[dim]" + repr(arg) + "[/]"
        elif isinstance(arg, Executor) or (isinstance(arg, type) and issubclass(arg, Executor)):
            return "[dim]" + repr(arg) + "[/]"
        elif isinstance(arg, rdflib.URIRef):
            return "[yellow]" + repr(arg) + "[/]"
        elif isinstance(arg, pathlib.Path):
            return "[blue]" + repr(arg) + "[/]"
        elif isinstance(arg, (str, int, float)):
            return arg
        else:
            return "[light]" + repr(arg) + "[/]"
        

    def format(self, record: logging.LogRecord) -> str:
        record.loglevelcolor = {
            'WARNING': '\033[31m',
            'INFO': '\033[32m',
            'DEBUG': '\033[35m',
        }[record.levelname]        

        
        if isinstance(record.args, tuple):
            record.args = tuple([self.repr_arg(a) for a in record.args])
        
        # print(">>>", record.args.__class__, record.args, isinstance(record.args[0], LocalURICachingExecutor))
        
        s = super().format(record)                
        
        for style, color in [
            ("red", "31"),
            ("r", "31"),
            ("b", "1;35"),
            ("dim", "1;30"),
            ("light", "37"),
            ("yellow", "2;33"),
            ("blue", "34"),
        ]:
            s = re.sub(rf"\[{style}\](.*?)\[/\]", rf"\033[{color}m\1\033[0m", s)
        
        return s
    
class AppLogging:
    @property
    def level_by_logger(self) -> dict:
        return getattr(self, '_level_by_logger', {"":"info"})

    @level_by_logger.setter
    def level_by_logger(self, level_by_logger: dict):
        self._level_by_logger = level_by_logger

    def parse_logspec(self, logspec):
        self.level_by_logger = dict([i.split(":", 1) for i in logspec.split(",")])        

    def __init__(self):
        pass


    def setup(self, tree=None) -> None:
        self.setup_tree(tree)


    def setup_tree(self, tree=None):
        if tree is None:
            tree = logging_tree.tree()

            if self.level_by_logger.get("logging_tree", "info").upper() == "DEBUG":
                logging_tree.printout()

        self.setup_formatter(tree[1])

        for n, l in self.level_by_logger.items():
            if re.match(n, tree[0]):
                logging.debug(f"setting logger \"{n}\" ({tree[1]}) at level {l}")
                try:
                    tree[1].setLevel(l.upper())
                except Exception as e:
                    logging.debug('can not set level for %s', tree[1])

        for child in tree[2]:
            self.setup_tree(child)


    def setup_formatter(self, logger):        
        for handler in getattr(logger, 'handlers', []):
            handler.setFormatter(ODAFunctionFormatter())


    def getLogger(self, *a, **aa):
        logger = logging.getLogger(*a, **aa)
        self.setup()
        return logger

app_logging = AppLogging()