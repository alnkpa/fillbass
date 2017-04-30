# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import os

import bs4
import click
import dateutil.parser
import math
import matplotlib
import matplotlib.pyplot as plt
import numpy
import sqlalchemy
from matplotlib import patches
from mpl_toolkits.mplot3d import Axes3D, art3d
from mpl_toolkits.mplot3d.art3d import Line3DCollection
from sqlalchemy.sql import func

from . import entities

matplotlib.rcParams['backend'] = "Qt5Agg"

LOG = logging.getLogger(__name__)


class DatabaseManager(object):
    """sets up a database and provides convenience functions"""

    def __init__(self, db_path, use_mysql):
        super(DatabaseManager, self).__init__()
        self.db_path = db_path
        self.pitch_count = 0
        if use_mysql:
            myDB = sqlalchemy.engine.url.URL(drivername='mysql',
                                             host='localhost',
                                             query={
                                                 'read_default_file': '~/.my.cnf'}
                                             )
            self.engine = sqlalchemy.create_engine(name_or_url=myDB)
        else:
            self.engine = sqlalchemy.create_engine(
                'sqlite:///' + self.db_path, echo=False)
        self.setup_db()
        self.session = sqlalchemy.orm.sessionmaker(bind=self.engine)()

    def setup_db(self):
        entities.Entity.metadata.create_all(self.engine)

    def add_at_bat(self, at_bat):
        self.session.add(at_bat)

    def add_at_bats(self, at_bats):
        self.session.add_all(at_bats)

    def add_player(self, player):
        self.session.add(player)

    def add_players(self, players):
        self.session.add_all(players)

    def add_pitches(self, pitches):
        self.pitch_count += len(pitches)
        LOG.info("Added {} pitches".format(len(pitches)))
        self.session.add_all(pitches)

    def player_present(self, pid):
        return bool(self.get_player(pid))

    def commit(self):
        self.session.commit()

    def get_players(self, first_name=None, last_name=None):
        query = self.session.query(entities.Player)
        if first_name is not None:
            query = query.filter(
                entities.Player.first_name.like(first_name + "%"))
        if last_name is not None:
            query = query.filter(
                entities.Player.last_name.like(last_name + "%"))

        return query.all()

    def get_player(self, id):
        return self.session.query(entities.Player).filter_by(pid=id).first()

    def get_average_for_pitches(self, column, pitcher_id=None, pitch_type=None):
        query = self.session.query(func.avg(column))
        if pitcher_id is not None:
            query = query.filter(entities.Pitch.pitcher == pitcher_id)
        if pitch_type is not None:
            query = query.filter(entities.Pitch.pitch_type.like(pitch_type))

        return query.one()

    def get_pitches(self, pitcher_id=None, pitch_type=None):
        query = self.session.query(entities.Pitch)
        if pitcher_id is not None:
            query = query.filter(entities.Pitch.pitcher == pitcher_id)
        if pitch_type is not None:
            query = query.filter(entities.Pitch.pitch_type.like(pitch_type))

        return query.all()

    def get_pitch_types(self, pitcher_id=None):
        query = self.session.query(entities.Pitch.pitch_type)
        if pitcher_id is not None:
            query = query.filter(entities.Pitch.pitcher == pitcher_id)
        return query.distinct().all()


class Parser(object):
    """parses game and player files"""

    import datetime

    def __init__(self, db):
        super(Parser, self).__init__()
        self.db = db
        self.parsed_players = []

    PITCH_MAPPINGS = {
        "result": "type"
    }

    PLAYER_MAPPINGS = {
        "pid": "id"
    }

    TYPE_TO_FROM_STRING = {
        int: lambda s: int(s),
        float: lambda s: float(s),
        datetime.datetime: lambda s: dateutil.parser.parse(s)
    }

    @staticmethod
    def map_or_keep(obj, mapping):
        return mapping[obj] if obj in mapping else obj

    @staticmethod
    def parse_class(clazz, xml, attribute_mapping):
        obj = {}
        for column in clazz.__table__.columns:
            attribute = Parser.map_or_keep(column.name, attribute_mapping)
            LOG.info("Searching for [%s]", attribute)
            if attribute in xml.attrs:
                value = None
                try:
                    value = Parser.map_or_keep(
                        column.type.python_type, Parser.TYPE_TO_FROM_STRING)(xml[attribute])
                except Exception:
                    pass
                LOG.info("Found [%s] of type [%s]", value, type(value))
                obj[column.name] = value
        return obj

    def parse_game(self, path):
        strain_atbats = bs4.SoupStrainer("atbat")
        pitches = []
        with open(path) as f:
            doc = bs4.BeautifulSoup(f, "xml", parse_only=strain_atbats)
            for atbat in doc.find_all("atbat"):
                pitcher = int(atbat["pitcher"])
                batter = int(atbat["batter"])
                for pitch in atbat.find_all("pitch"):
                    try:
                        pitch_dict = Parser.parse_class(
                            entities.Pitch, pitch, Parser.PITCH_MAPPINGS)
                        pitch_dict["pitcher"] = pitcher
                        pitch_dict["batter"] = batter
                        pitches.append(entities.Pitch(**pitch_dict))
                    except Exception as e:
                        LOG.warning("Encountered error [%s] while parsing a pitch", e)
            self.db.add_pitches(pitches)

    def parse_player(self, path):
        with open(path) as f:
            doc = bs4.BeautifulSoup(f, "xml")
            for player in doc.find_all("Player"):
                if not int(player["id"]) in self.parsed_players:
                    try:
                        player_dict = Parser.parse_class(
                            entities.Player, player, Parser.PLAYER_MAPPINGS)
                        self.db.add_player(entities.Player(**player_dict))
                        self.parsed_players.append(int(player["id"]))
                    except Exception as e:
                        LOG.warning("Encountered error [%s] while parsing player [%s]", e, int(player["id"]))

    def find_files(self, directory):
        file_list = list(os.walk(directory))
        with click.progressbar(file_list, label="Scanning all files", width=0,
                               item_show_func=lambda x: x[0] if x is not None else None) as file_tuples:
            for root, _, files in file_tuples:
                for name in files:
                    file_name = os.path.join(root, name)
                    LOG.debug("now parsing file [%s]", name)
                    if name.startswith("inning_all"):
                        self.parse_game(file_name)
                    elif name[:1].isdigit():
                        self.parse_player(file_name)
                self.db.commit()


class Drawer(object):
    """draws nice graphs"""

    def __init__(self, db):
        super(Drawer, self).__init__()
        self.db = db

    def pitches_by_type(self, pitcher, pitch_type=None):
        pitch_types = self.db.get_pitch_types(pitcher_id=pitcher.pid)

        fig = plt.figure()
        ax = fig.add_subplot(111, projection="3d")

        pitch_count = 0
        sz_bot = self.db.get_average_for_pitches(entities.Pitch.sz_bot, pitcher.pid, pitch_type)[0]
        sz_top = self.db.get_average_for_pitches(entities.Pitch.sz_top, pitcher.pid, pitch_type)[0]

        for t in pitch_types:
            if pitch_type is not None and not t[0] == pitch_type:
                LOG.info("Type [%s] does not match selected type [%s]", t[0], pitch_type)
                continue

            pitches = self.db.get_pitches(
                pitcher_id=pitcher.pid, pitch_type=t[0])

            lines = []
            for pitch in pitches:
                if pitch.x0 is None or \
                                pitch.y0 is None or \
                                pitch.z0 is None or \
                                pitch.px is None or \
                                pitch.pz is None or \
                                pitch.vx0 is None or \
                                pitch.vy0 is None or \
                                pitch.vz0 is None or \
                                pitch.ax is None or \
                                pitch.ay is None or \
                                pitch.az is None:
                    continue

                t_to_catcher = (- pitch.vy0 - math.sqrt((pitch.vy0**2)-2*pitch.ay*(pitch.y0-0))) / pitch.ay
                pitch_line = [(pitch.x0 + pitch.vx0 * t + pitch.ax * t**2 / 2,
                               pitch.y0 + pitch.vy0 * t + pitch.ay * t**2 / 2,
                               pitch.z0 + pitch.vz0 * t + pitch.az * t**2 / 2)
                              for t in numpy.linspace(0, t_to_catcher)]
                lines.append(pitch_line)
            ax.add_collection(Line3DCollection(lines, label=t[0], linewidths=1, alpha=0.5, colors=next(ax._get_lines.prop_cycler)['color']))

            pitch_count += len(pitches)

        strikezone = patches.Rectangle((-0.7083, sz_bot), 0.7083 * 2, sz_top - sz_bot, fill=False, label="Strikezone")

        ax.add_patch(strikezone)
        art3d.pathpatch_2d_to_3d(strikezone, z=1.417, zdir="y")
        ax.set_xlim(-0.7083 * 4, 0.7083 * 4)
        ax.set_ylim(0, 50)
        ax.set_zlim(-1, sz_top * 2)
        ax.set_title("Pitch Location by type for " + str(pitcher))
        ax.legend()
        LOG.info("Evaluated [%i] pitches", pitch_count)
        plt.show(block=True)
