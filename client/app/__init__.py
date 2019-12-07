from datetime import datetime
from threading import Thread
from time import sleep
from flask import Flask
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from flask_cors import CORS
from rpyc.utils.server import ThreadedServer
from sqlalchemy.exc import SQLAlchemyError
from .config import config
from .models import db, ContactModel, GroupModel, MessageModel, UserModel
from ..service import ClientService
from ...server import UserData


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

    thread = Thread(target=get_messages, args=(app, ))
    thread.start()

    admin.add_view(ModelView(ContactModel, db.session))
    admin.add_view(ModelView(GroupModel, db.session))
    admin.add_view(ModelView(MessageModel, db.session))
    admin.add_view(ModelView(UserModel, db.session))

    from .controllers.main import main_blueprint
    app.register_blueprint(main_blueprint)

    return app


def start_service(app):
    service = ClientService(app)
    server = ThreadedServer(service, port=3000, protocol_config={'allow_public_attrs': True})
    server.start()


def get_messages(app):
    sleep(10)
    while True:
        with app.app_context():
            user = UserModel.query.first()
            if user:
                result = ClientService.get_user_data(int(user.tracker_id), True)
                if result:
                    user = UserData.from_json(result)
                    for message in user.get_messages():
                        m = MessageModel(message.text, time=datetime.strptime(message.time, '%Y-%m-%d %H:%M:%S.%f'))
                        c = ContactModel.query.filter_by(tracker_id=str(message.sender)).first()
                        if not c:
                            result = ClientService.get_user_data(message.sender)
                            if not result:
                                continue
                            user_data = UserData.from_json(result)
                            c = ContactModel(user_data.get_id(), user_data.get_phone(), user_data.get_name()[0], *user_data.get_dir()[0])
                        m.sender = c
                        try:
                            app.db.session.add(c)
                            app.db.session.commit()
                        except SQLAlchemyError:
                            app.db.session.rollback()
        sleep(5)
