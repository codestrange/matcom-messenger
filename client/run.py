from os import getenv
from logging import basicConfig, DEBUG
from .app import create_app
from .app.models import db, ContactModel, GroupModel, MessageModel, UserModel

basicConfig(filename=f'file.log', filemode='w', format='%(asctime)s - %(levelname)s - %(name)s: %(message)s', level=DEBUG)

app = create_app(getenv('FLASK_CONFIG') or 'default')


@app.shell_context_processor
def make_shell_context():
    return dict(app=app, db=db, ContactModel=ContactModel, GroupModel=GroupModel, \
        MessageModel=MessageModel, UserModel=UserModel)


@app.cli.command()
def init():
    db.drop_all()
    db.create_all()
