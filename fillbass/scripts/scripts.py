#!/usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import click
import requests
from requests.adapters import HTTPAdapter
from tabulate import tabulate

from fillbass.entities import Player
from fillbass.fetchdata import fetch_day
from fillbass.parsedata import DatabaseManager, Parser, Drawer

ONE_DAY = timedelta(days=1)
LOG = logging.getLogger(__name__)
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS, chain=True)
@click.option("-v", "--verbose", count=True)
@click.option("-d", "--database", type=click.Path(dir_okay=False, writable=True), default="fillbass.db",
              help="""use this file as the sqlite database file. Might be written when
                      using scan. Defaults to 'fillbass.db'""")
@click.option("--mysql/--no-mysql", default=False, help="""Use MySQL as database.
If True, database access needs to be configured via ~/.my.cnf. If False, use sqlite. Default to False.""")
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
@click.option("-j", "--jobs", metavar="COUNT", help="""use COUNT jobs for downloading. If not given,
it will default to the number of processors on the machine, multiplied by 5.""", type=click.IntRange(min=1),
              default=None)
@click.argument("save_path", nargs=1, type=click.Path(exists=False, file_okay=False, dir_okay=True, writable=True),
                default="data")
def fetch(start_date, end_date, jobs, save_path):
    start_date = datetime.strptime(start_date, "%d/%m/%Y").date()
    end_date = datetime.strptime(end_date, "%d/%m/%Y").date()

    players_fetched = []

    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            adapter = HTTPAdapter(pool_connections=executor._max_workers * 2, pool_block=True)
            session.mount("http://", adapter)
            while start_date <= end_date:
                try:
                    executor.submit(fetch_day, save_path, start_date, session,
                                    players_fetched)
                except Exception as e:
                    LOG.error("Encountered [%s] while fetching day [%s]", e, start_date)
                finally:
                    start_date += ONE_DAY


@cli.command(help="scan and parse a directory tree for XML files")
@click.argument("directory", nargs=1, type=click.Path(exists=True, file_okay=False), default="data")
@click.pass_context
def scan(ctx, directory):
    if "DB_MANAGER" not in ctx.obj:
        ctx.obj["DB_MANAGER"] = DatabaseManager(ctx.obj["DATABASE"], ctx.obj["MYSQL"])
    db_manager = ctx.obj["DB_MANAGER"]
    parser = Parser(db_manager)
    parser.find_files(directory)


@cli.command(help="list players")
@click.option("-f", "--first-name", help="""first name of the player""", type=str, default=None)
@click.option("-l", "--last-name", help="""last name of the player""", type=str, default=None)
@click.pass_context
def list(ctx, first_name, last_name):
    if "DB_MANAGER" not in ctx.obj:
        ctx.obj["DB_MANAGER"] = DatabaseManager(ctx.obj["DATABASE"], ctx.obj["MYSQL"])
    db_manager = ctx.obj["DB_MANAGER"]
    matching_players = db_manager.get_players(first_name, last_name)
    column_names = [n for n in map(lambda c: c.name, Player.__table__.columns)]

    players_table = tabulate([{c: getattr(player, c) for c in column_names} for player in matching_players], headers="keys")

    if len(matching_players) > 1:
        click.echo_via_pager(players_table)
    else:
        click.echo(players_table)

    if len(matching_players) is 1:
        ctx.obj["CURRENT_PLAYER"] = matching_players[0]


@cli.command(help="show all pitches by a pitcher")
@click.argument("player_id", type=str, required=False, nargs=1, default=None)
@click.option("-p", "--pitch-type", help="""only show pitches of this type""", type=str, default=None)
@click.pass_context
def pitches_by(ctx, player_id, pitch_type):
    if player_id is None:
        if "CURRENT_PLAYER" in ctx.obj:
            player_id = ctx.obj["CURRENT_PLAYER"].pid
        else:
            click.echo("Please provide a player_id or chain with a list call that finds exactly one player.")
            return

    if "DB_MANAGER" not in ctx.obj:
        ctx.obj["DB_MANAGER"] = DatabaseManager(ctx.obj["DATABASE"], ctx.obj["MYSQL"])

    db_manager = ctx.obj["DB_MANAGER"]
    drawer = Drawer(db_manager)
    drawer.pitches_by_type(db_manager.get_player(player_id), pitch_type)


if __name__ == "__main__":
    cli(obj={})
