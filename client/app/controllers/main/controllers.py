from flask import render_template, redirect, url_for
from . import main_blueprint
from .forms import RegisterForm
from ...models import db, UserModel
from ....service import ClientService
from .....server.tracker import TrackerService


@main_blueprint.route('/', methods=['GET'])
def index():
    if UserModel.query.first() is None:
        return redirect(url_for('main.register'))
    return render_template('index.html')


@main_blueprint.route('/register', methods=['GET', 'POST'])
def register():
    form = RegisterForm()
    if form.validate_on_submit():
        phone = form.phone.data
        name = form.name.data
        user = UserModel(phone, name)
        result = ClientService.store_user_data(phone, 0, name, '1234', TrackerService.get_ip(), 3000)
        if not result:
            pass
        db.session.add(user)
        db.session.commit()
        return redirect(url_for('main.index'))
    return render_template('register.html', form=form)


@main_blueprint.app_errorhandler(403)
def forbidden(e):
    return render_template('errors/403.html'), 403


@main_blueprint.app_errorhandler(404)
def page_not_found(e):
    return render_template('errors/404.html'), 404


@main_blueprint.app_errorhandler(500)
def internal_server_error(e):
    return render_template('errors/500.html'), 500
