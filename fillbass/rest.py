# Eve imports
from eve import Eve
from eve_sqlalchemy import SQL
from eve_sqlalchemy.validation import ValidatorSQL

# Eve-SQLAlchemy imports
from eve_sqlalchemy.decorators import registerSchema

import entities

registerSchema('player')(entities.Player)
registerSchema('pitch')(entities.Pitch)

SETTINGS = {
    'DEBUG': True,
    'SQLALCHEMY_DATABASE_URI': 'sqlite:///./fillbass.db',
    'DOMAIN': {
        'people': entities.Player._eve_schema['player'],
        'pitch': entities.Pitch._eve_schema['pitch']
        }
}

print(entities.Player._eve_schema['player'])

app = Eve(auth=None, settings=SETTINGS, validator=ValidatorSQL, data=SQL)

# bind SQLAlchemy
db = app.data.driver
entities.Entity.metadata.bind = db.engine
db.Model = entities.Entity
db.create_all()

# Insert some example data in the db
# using reloaded will destory in-memory sqlite db
