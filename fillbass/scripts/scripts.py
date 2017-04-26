import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import click
import requests
from requests.adapters import HTTPAdapter

from fillbass.fetchdata import fetch_day
from fillbass.parsedata import DatabaseManager, Parser

ONE_DAY = timedelta(days=1)
LOG = logging.getLogger(__name__)
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option("-v", "--verbose", count=True)
@click.option("-d", "--database", default="fillbass.db",
                        help="""use this file as the sqlite database file. Might be written when
                      using scan. Defaults to 'fillbass.db'""")
@click.option("--mysql/--no-mysql", default=True, help="""Use MySQL as database.
If True, database access needs to be configured via ~/.my.cnf. If False, use sqlite. Default to True.""")
@click.pass_context
def cli(ctx, verbose, database, mysql):
    ctx.obj = {}
    log_level = logging.ERROR

    if verbose >= 2:
        log_level = logging.INFO
    elif verbose >= 1:
        log_level = logging.DEBUG

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s] %(message)s", level=log_level)

    ctx.obj["DATABASE"] = database
    ctx.obj["MYSQL"] = mysql


@cli.command(help="Downloads XML files describing all games in the specified time-frame")
@click.option("-s", "--start-date", help="""fetch data beginning from this day. Format as 'DD/MM/YYYY'.
                                                Defaults to 01/01/2008""",
              default="01/01/2008")
@click.option("-e", "--end-date", help="""fetch data up to and including this day. Format as 'DD/MM/YYYY'.
                                              Defaults to 01/01/2017""",
              default="01/01/2017")
@click.argument("save_path", nargs=1,
                default="data")
def fetch(start_date, end_date, save_path):
    start_date = datetime.strptime(start_date, "%d/%m/%Y").date()
    end_date = datetime.strptime(end_date, "%d/%m/%Y").date()

    players_fetched = []

    with requests.Session() as session:
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=100)
        session.mount("http://", adapter)

        with ThreadPoolExecutor() as executor:
            while start_date <= end_date:
                try:
                    executor.submit(fetch_day, save_path, start_date, session,
                                    players_fetched)
                except Exception as e:
                    LOG.error("Encountered [%s] while fetching day [%s]", e, start_date)
                finally:
                    start_date += ONE_DAY


@cli.command(help="scan and parse a directory tree for XML files")
@click.argument("directory", nargs=1, default="data")
@click.pass_context
def scan(ctx, directory):
    db_manager = DatabaseManager(ctx.obj["DATABASE"], ctx.obj["MYSQL"])
    parser = Parser(db_manager)
    parser.find_files(directory)
