import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base

Entity = declarative_base()

class Player(Entity):
	"""a single player"""
	__tablename__ = "players"

	pid = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
	pos = sqlalchemy.Column(sqlalchemy.String)
	first_name = sqlalchemy.Column(sqlalchemy.String)
	last_name = sqlalchemy.Column(sqlalchemy.String)
	bats = sqlalchemy.Column(sqlalchemy.String)
	throws = sqlalchemy.Column(sqlalchemy.String)
	dob = sqlalchemy.Column(sqlalchemy.String)

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
	sv_id = sqlalchemy.Column(sqlalchemy.String)
	pitch_type = sqlalchemy.Column(sqlalchemy.String)
	type_confidence = sqlalchemy.Column(sqlalchemy.Float)
	pitcher = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey("players.pid"))

	def __init__(self, px, pz, x0, z0, y0, sv_id, pitch_type, type_confidence, pitcher):
		super(Pitch, self).__init__()
		self.px = px
		self.pz = pz
		self.x0 = x0
		self.z0 = z0
		self.y0 = y0
		self.sv_id = sv_id
		self.pitch_type = pitch_type
		self.type_confidence = type_confidence
		self.pitcher = pitcher
