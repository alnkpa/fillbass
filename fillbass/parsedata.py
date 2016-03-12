import bs4
import os
import sqlalchemy
import entities
import matplotlib.pyplot as plt
import argparse
import dateutil.parser
import logging

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.FileHandler("parse.log")
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter(
    '%(asctime)s %(name)s [%(levelname)s] %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
LOG.addHandler(ch)


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

    def add_player(self, player):
        self.session.add(player)

    def add_players(self, players):
        self.session.add_all(players)

    def add_pitches(self, pitches):
        self.pitch_count += len(pitches)
        LOG.info("Added {} pitches".format(len(pitches)))
        self.session.add_all(pitches)

    def player_present(self, pid):
        return bool(self.session.query(entities.Player)
                    .filter_by(pid=pid)
                    .first())

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

    def map_or_keep(obj, mapping):
        return mapping[obj] if obj in mapping else obj

    def parse_class(clazz, xml, attribute_mapping):
        obj = {}
        for column in clazz.__table__.columns:
            attribute = Parser.map_or_keep(column.name, attribute_mapping)
            LOG.debug("Searching for {}".format(attribute))
            if attribute in xml.attrs:
                value = Parser.map_or_keep(
                    column.type.python_type, Parser.TYPE_TO_FROM_STRING)(xml[attribute])
                LOG.debug("Found {} of type {}".format(value, type(value)))
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
                        LOG.warning("Encountered {}".format(e))
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
                        LOG.warning("Encountered {}".format(e))

    def find_files(self, directory):
        for root, _, files in os.walk(directory):
            for name in files:
                file_name = os.path.join(root, name)
                LOG.debug("now parsing file {}".format(name))
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

    def forceAspect(ax, aspect=1):
        im = ax.get_images()
        extent = im[0].get_extent()
        ax.set_aspect(
            abs((extent[1] - extent[0]) / (extent[3] - extent[2])) / aspect)

    def pitches_by_type(self, pitcher):
        pitch_types = self.db.get_pitch_types(pitcher_id=pitcher.pid)

        fig = plt.figure()
        ax = fig.add_subplot(111)

        pitch_count = 0

        for t in pitch_types:
            pitches = self.db.get_pitches(
                pitcher_id=pitcher.pid, pitch_type=t[0])
            pitch_count += len(pitches)
            ax.plot([x.px for x in pitches],
                    [x.pz for x in pitches],
                    ".",
                    label=t[0])
        ax.hlines(y=0, xmin=-0.7083, xmax=0.7083, label="Home plate")
        ax.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
        ax.set_aspect(1)
        ax.set_title("Pitch Location by type for " + str(pitcher))
        print("evaluated %i pitches" % pitch_count)
        plt.show()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subparser_name")

    scan = subparsers.add_parser("scan", help="""scan and parse a directory tree for
                                                 xml files""")
    scan.add_argument("directory", nargs="?", default="data",
                      help="""walk this directory. Defaults to 'data'""")

    list_players = subparsers.add_parser("list", help="""list any players""")
    list_players.add_argument("first_name", nargs="?", default=None)
    list_players.add_argument("last_name", nargs="?", default=None)

    show_pitches = subparsers.add_parser("show", help="show pitches")
    show_pitches.add_argument(
        "pitcher", type=int, help="""id of the wanted pitcher""")
    parser.add_argument("-d", "--database", default="fillbass.db",
                        help="""use this file as the database. Might be written when
                      using scan. Defaults to 'fillbass.db'""")
    parser.add_argument('--mysql', dest='mysql', action='store_true')
    parser.add_argument('--no-mysql', dest='mysql', action='store_false')
    parser.set_defaults(mysql=True)
    args = parser.parse_args()

    db = DatabaseManager(args.database, args.mysql)
    draw = Drawer(db)
    parser = Parser(db)

    if args.subparser_name == "scan":
        parser.find_files(args.directory)
    elif args.subparser_name == "list":
        print("ID\tName")
        players = db.get_players(first_name=args.first_name,
                                 last_name=args.last_name)
        for p in players:
            print("%i\t%s" % (p.pid, p))
    elif args.subparser_name == "show":
        draw.pitches_by_type(db.get_player(args.pitcher))
