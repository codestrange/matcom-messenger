from functools import wraps
from flask import current_app, redirect, url_for
from sqlalchemy.exc import OperationalError
from .models import UserModel


def register_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = None
        try:
            user = UserModel.query.first()
        except OperationalError:
            current_app.db.drop_all()
            current_app.db.create_all()
            user = UserModel.query.first()
        if not user:
            return redirect(url_for('main.register'))
        return f(*args, **kwargs)
    return decorated_function
