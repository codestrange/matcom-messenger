from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import check_password_hash, generate_password_hash
from ...server import get_hash


db = SQLAlchemy(session_options={"autoflush": False})


class UserModel(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    phone = db.Column(db.String(64), unique=True, nullable=False)
    tracker_id = db.Column(db.String(256), unique=True, nullable=False)
    name = db.Column(db.String(64), nullable=False)

    def __init__(self, phone, name):
        self.name = name
        self.phone = phone
        self.tracker_id = str(get_hash(f'{phone}:0'))

    def __repr__(self):
        return self.name


class ContactModel(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tracker_id = db.Column(db.String(256), unique=True, nullable=False)
    phone = db.Column(db.String(64), unique=True, nullable=False)
    name = db.Column(db.String(64), nullable=False)
    ip = db.Column(db.String(64), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    messages = db.relationship('MessageModel', backref='sender', lazy='dynamic')

    def __init__(self, tracker_id, phone, name, ip, port):
        self.tracker_id = str(tracker_id)
        self.name = name
        self.phone = phone
        self.ip = ip
        self.port = port

    def __repr__(self):
        return self.name


class MessageModel(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    text = db.Column(db.Text, nullable=False)
    time = db.Column(db.DateTime, default=datetime.now())
    sender_id = db.Column(db.Integer, db.ForeignKey('contact_model.id'))
    received = db.Column(db.Boolean, default=True, index=True)

    def __init__(self, text, received = True, time = None):
        self.text = text
        self.received = received
        if time:
            self.time = time

    def __repr__(self):
        return self.text
