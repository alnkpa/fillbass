import bs4
import os

import click
import sqlalchemy
from matplotlib import patches
from sqlalchemy.sql import func
from . import entities
import matplotlib.pyplot as plt
import argparse
import dateutil.parser
import logging
import matplotlib
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
        with click.progressbar(file_list, label="Scanning all files", width=0, item_show_func=lambda x: x[0] if x is not None else None) as file_tuples:
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
        ax = fig.add_subplot(111)

        pitch_count = 0
        sz_bot = self.db.get_average_for_pitches(entities.Pitch.sz_bot, pitcher.pid, pitch_type)[0]
        sz_top = self.db.get_average_for_pitches(entities.Pitch.sz_top, pitcher.pid, pitch_type)[0]

        for t in pitch_types:
            if pitch_type is not None and not t[0] == pitch_type:
                LOG.info("Type [%s] does not match selected type [%s]", t[0], pitch_type)
                continue

            pitches = self.db.get_pitches(
                pitcher_id=pitcher.pid, pitch_type=t[0])
            pitch_count += len(pitches)
            ax.plot([x.px for x in pitches],
                    [x.pz for x in pitches],
                    ".",
                    ms=4,
                    label=t[0],
                     alpha=0.75)

        ax.add_patch(patches.Rectangle((-0.7083, sz_bot), 0.7083*2, sz_top-sz_bot, fill=False, label="Strikezone", zorder=100))
        ax.set_xlim(-0.7083*4, 0.7083*4)
        ax.set_ylim(-1, sz_top*2)
        ax.set_aspect(1)
        ax.set_title("Pitch Location by type for " + str(pitcher))
        ax.legend(loc="upper left", bbox_to_anchor=(1,1))
        LOG.info("Evaluated [%i] pitches", pitch_count)
        plt.show(block=True)
