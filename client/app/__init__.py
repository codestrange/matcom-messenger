from threading import Thread
from flask import Flask
from flask_cors import CORS
from .config import config
from .models import db, User


def create_app(config_name):
    app = Flask(__name__)
    CORS(app)
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)

    db.init_app(app)
    app.db = db

    thread = Thread(target=start_service, args=(app, ))
    thread.start()

    from .controllers.main import main_blueprint
    app.register_blueprint(main_blueprint)

    return app


def start_service(app):
    pass
