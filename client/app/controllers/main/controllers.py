from flask import render_template, redirect, url_for
from . import main_blueprint
from ...models import UserModel


@main_blueprint.route('/', methods=['GET'])
def index():
    if UserModel.query.first() is None:
        return redirect(url_for('main.register'))
    return render_template('index.html')


@main_blueprint.route('/register', methods=['GET', 'POST'])
def register():
    return 'Register page'


@main_blueprint.app_errorhandler(403)
def forbidden(e):
    return render_template('errors/403.html'), 403


@main_blueprint.app_errorhandler(404)
def page_not_found(e):
    return render_template('errors/404.html'), 404


@main_blueprint.app_errorhandler(500)
def internal_server_error(e):
    return render_template('errors/500.html'), 500
