import click
import logging


@click.group
def main():
    logging.basicConfig(level='DEBUG')


@main.command()
@click.argument("uri")
@click.option("-nc", "--no-cache", is_flag=True)
def run(uri, no_cache):
    from odafunction.executors import default_execute_to_local_value
    from odafunction.func.urifunc import URIFunction

    default_execute_to_local_value(URIFunction.from_uri(uri)(), cached=not no_cache)


if __name__ == "__main__":
    main()