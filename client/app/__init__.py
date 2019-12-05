from threading import Thread
from flask import Flask
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from flask_cors import CORS
from rpyc.utils.server import ThreadedServer
from .config import config
from .models import db, ContactModel, MessageModel, UserModel
from ..service import ClientService

admin = Admin()


def create_app(config_name):
    app = Flask(__name__)
    CORS(app)
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)

    admin.init_app(app)
    db.init_app(app)
    app.db = db

    thread = Thread(target=start_service, args=(app, ))
    thread.start()

    admin.add_view(ModelView(ContactModel, db.session))
    admin.add_view(ModelView(MessageModel, db.session))
    admin.add_view(ModelView(UserModel, db.session))

    from .controllers.main import main_blueprint
    app.register_blueprint(main_blueprint)

    return app


def start_service(app):
    service = ClientService(app)
    server = ThreadedServer(service, port=3000, protocol_config={'allow_public_attrs': True})
    server.start()
