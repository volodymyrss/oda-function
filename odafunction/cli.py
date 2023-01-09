import click
import logging

from .utils import repr_trim

from . import logs
from .executors import default_execute_to_value
from .func.urifunc import URIFunction, URIValue, LocalValue


@click.group()
@click.option('-v', is_flag=True)
@click.option('-vv', is_flag=True)
@click.option('-l', '--logspec', default=None)
def main(v, vv, logspec):
    if vv:
        level = 'DEBUG'
    elif v:
        level = 'INFO'
    else:
        level = 'WARNING'
    
    logging.basicConfig(level=level)
    
    if logspec is not None:
        logs.app_logging.level_by_logger = dict([i.split(":", 1) for i in logspec.split(",")])        

    logs.app_logging.setup()


@main.command()
@click.argument("uri")
@click.option("-nc", "--no-cache", is_flag=True)
@click.option("-i", "--inplace", is_flag=True)
@click.option("-u", "--urivalue", is_flag=True)
def run(uri, no_cache, inplace, urivalue):

    f = URIFunction.from_uri(uri)()

    # TODO: inplace should be executor option!
    # f.inplace = inplace

    v = default_execute_to_value(f, 
                                 cached=not no_cache, 
                                 valueclass=URIValue if urivalue else LocalValue)
    logging.info("function returns: %s", repr_trim(v))    


if __name__ == "__main__":
    main(auto_envvar_prefix="ODAFUNCTION")