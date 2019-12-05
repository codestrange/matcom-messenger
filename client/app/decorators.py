from functools import wraps
from flask import redirect, url_for
from .models import UserModel


def register_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if UserModel.query.first() is None:
            return redirect(url_for('main.register'))
        return f(*args, **kwargs)
    return decorated_function
