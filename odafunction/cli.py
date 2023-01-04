import click
import logging

from . import logs
from .executors import default_execute_to_value
from .func.urifunc import URIFunction


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

    
    logging.basicConfig(
        level=level        
    )


    
    if logspec is not None:
        logs.app_logging.level_by_logger = dict([i.split(":", 1) for i in logspec.split(",")])        

    logs.app_logging.setup()


@main.command()
@click.argument("uri")
@click.option("-nc", "--no-cache", is_flag=True)
@click.option("-i", "--in-place", is_flag=True)
def run(uri, no_cache, in_place):

    f = URIFunction.from_uri(uri)()

    # TODO: inplace should be executor option!
    f.inplace = in_place

    v = default_execute_to_value(f, cached=not no_cache)
    logging.info("function returns: %s", v)    


if __name__ == "__main__":
    main(auto_envvar_prefix="ODAFUNCTION")