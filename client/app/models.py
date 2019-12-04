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


class ContactModel(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tracker_id = db.Column(db.Integer, unique=True, nullable=False)
    name = db.Column(db.String(64), nullable=False)
    host = db.Column(db.String(64), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    messages = db.relationship('MessageModel', backref='sender', lazy='dynamic')

    def __init__(self, tracker_id, name, host, port):
        self.tracker_id = tracker_id
        self.name = name
        self.host = host
        self.port

    def __repr__(self):
        return self.name if self.name else f'<{host}, {port}, {tracker_id}>'


class MessageModel(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    text = db.Column(db.Text, nullable=False)
    time = db.Column(db.DateTime, nullable=False)
    sender_id = db.Column(db.Integer, db.ForeignKey('contact_model.id'))

    def __init__(self, text, time):
        self.text = text
        self.time = time

    def __repr__(self):
        return self.text
