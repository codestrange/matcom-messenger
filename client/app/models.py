from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import check_password_hash, generate_password_hash
from ...server import get_hash


db = SQLAlchemy(session_options={"autoflush": False})


class UserModel(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    phone = db.Column(db.String(64), unique=True, nullable=False)
    tracker_id = db.Column(db.Integer, unique=True, nullable=False)
    name = db.Column(db.String(64), nullable=False)

    def __init__(self, phone, name):
        self.name = name
        self.phone = phone
        self.tracker_id = get_hash(f'{phone}:0')

    def __repr__(self):
        return self.name
