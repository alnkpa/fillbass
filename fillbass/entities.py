import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base

Entity = declarative_base()


class Player(Entity):
    """a single player"""
    __tablename__ = "players"

    pid = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    pos = sqlalchemy.Column(sqlalchemy.String(length=50))
    first_name = sqlalchemy.Column(sqlalchemy.String(length=50))
    last_name = sqlalchemy.Column(sqlalchemy.String(length=50))
    bats = sqlalchemy.Column(sqlalchemy.String(length=50))
    throws = sqlalchemy.Column(sqlalchemy.String(length=50))
    dob = sqlalchemy.Column(sqlalchemy.String(length=50))

    def __init__(self, pid, pos, first_name, last_name, bats, throws, dob):
        super(Player, self).__init__()
        self.pid = pid
        self.pos = pos
        self.first_name = first_name
        self.last_name = last_name
        self.bats = bats
        self.throws = throws
        self.dob = dob

    def __repr__(self):
        return self.first_name + " " + self.last_name


class Pitch(Entity):
    """a single pitch"""
    __tablename__ = "pitches"

    pid = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    px = sqlalchemy.Column(sqlalchemy.Float)
    pz = sqlalchemy.Column(sqlalchemy.Float)
    x0 = sqlalchemy.Column(sqlalchemy.Float)
    z0 = sqlalchemy.Column(sqlalchemy.Float)
    y0 = sqlalchemy.Column(sqlalchemy.Float)
    sv_id = sqlalchemy.Column(sqlalchemy.String(length=50))
    pitch_type = sqlalchemy.Column(sqlalchemy.String(length=5))
    type_confidence = sqlalchemy.Column(sqlalchemy.Float)
    pitcher = sqlalchemy.Column(sqlalchemy.Integer,
                                sqlalchemy.ForeignKey("players.pid"))
    batter = sqlalchemy.Column(sqlalchemy.Integer,
                                sqlalchemy.ForeignKey("players.pid"))
    des = sqlalchemy.Column(sqlalchemy.String(length=50))
    result = sqlalchemy.Column(sqlalchemy.Enum("S", "B", "X"))
    tfs_zulu = sqlalchemy.Column(sqlalchemy.DateTime)
    x = sqlalchemy.Column(sqlalchemy.Float)
    y = sqlalchemy.Column(sqlalchemy.Float)
    on_1b = sqlalchemy.Column(sqlalchemy.Integer,
                              sqlalchemy.ForeignKey("players.pid"))
    on_2b = sqlalchemy.Column(sqlalchemy.Integer,
                              sqlalchemy.ForeignKey("players.pid"))
    on_3b = sqlalchemy.Column(sqlalchemy.Integer,
                              sqlalchemy.ForeignKey("players.pid"))
    start_speed = sqlalchemy.Column(sqlalchemy.Float)
    end_speed = sqlalchemy.Column(sqlalchemy.Float)
    sz_top = sqlalchemy.Column(sqlalchemy.Float)
    sz_bot = sqlalchemy.Column(sqlalchemy.Float)
    pfx_x = sqlalchemy.Column(sqlalchemy.Float)
    pfx_z = sqlalchemy.Column(sqlalchemy.Float)
    vx0 = sqlalchemy.Column(sqlalchemy.Float)
    vy0 = sqlalchemy.Column(sqlalchemy.Float)
    vz0 = sqlalchemy.Column(sqlalchemy.Float)
    ax = sqlalchemy.Column(sqlalchemy.Float)
    ay = sqlalchemy.Column(sqlalchemy.Float)
    az = sqlalchemy.Column(sqlalchemy.Float)
    break_y = sqlalchemy.Column(sqlalchemy.Float)
    break_angle = sqlalchemy.Column(sqlalchemy.Float)
    break_length = sqlalchemy.Column(sqlalchemy.Float)
    type_confidence = sqlalchemy.Column(sqlalchemy.Float)
    zone = sqlalchemy.Column(sqlalchemy.Integer)
    nasty = sqlalchemy.Column(sqlalchemy.Integer)
    spin_dir = sqlalchemy.Column(sqlalchemy.Float)
    spin_rate = sqlalchemy.Column(sqlalchemy.Float)
    cc = sqlalchemy.Column(sqlalchemy.String(length=50))
    mt = sqlalchemy.Column(sqlalchemy.String(length=50))
