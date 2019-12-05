from os import getenv
from .app import create_app
from .app.models import db, ContactModel, MessageModel, UserModel


app = create_app(getenv('FLASK_CONFIG') or 'default')


@app.shell_context_processor
def make_shell_context():
    return dict(app=app, db=db, ContactModel=ContactModel, MessageModel=MessageModel, \
        UserModel=UserModel)


@app.cli.command()
def init():
    db.drop_all()
    db.create_all()
    c = ContactModel(1, '+5352464795', 'detnier', '127.0.0.1', 3000)
    db.session.add(c)
    c = ContactModel(2, '+5353634222', 'yordamis', '127.0.0.1', 3000)
    db.session.add(c)
    u = UserModel('+5353478301', 'leynier')
    db.session.add(u)
    db.session.commit()
