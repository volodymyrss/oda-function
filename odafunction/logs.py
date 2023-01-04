import re
import logging
import logging_tree


class Formatter(logging.Formatter):    
    def __init__(self) -> None:
        super().__init__("%(asctime)s %(loglevelcolor)s%(levelname)7.7s\033[0m \033[33m%(name)20.20s\033[0m %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        record.loglevelcolor = {
            'WARNING': '\033[31m',
            'INFO': '\033[32m',
            'DEBUG': '\033[34m',
        }[record.levelname]
         
        # record.msg =  re.sub(r"(\[.*?\])", r"\033[35m\1\033[0m", record.getMessage())
        return super().format(record)                
    
class AppLogging:
    @property
    def level_by_logger(self) -> dict:
        return getattr(self, '_level_by_logger', {"":"info"})

    @level_by_logger.setter
    def level_by_logger(self, level_by_logger: dict):
        self._level_by_logger = level_by_logger

    def __init__(self):
        pass


    def setup(self, tree=None) -> None:
        self.setup_tree(tree)


    def setup_tree(self, tree):
        if tree is None:
            tree = logging_tree.tree()

            if self.level_by_logger.get("logging_tree", "info").upper() == "DEBUG":
                logging_tree.printout()

        self.setup_formatter(tree[1])

        for n, l in self.level_by_logger.items():
            if n == tree[0]:
                logging.debug(f"setting logger \"{n}\" ({tree[1]}) at level {l}")
                tree[1].setLevel(l.upper())

        for child in tree[2]:
            self.setup_tree(child)


    def setup_formatter(self, logger):        
        for handler in getattr(logger, 'handlers', []):
            handler.setFormatter(Formatter())


    def getLogger(self, *a, **aa):
        logger = logging.getLogger(*a, **aa)
        self.setup()
        return logger

app_logging = AppLogging()