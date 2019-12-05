from datetime import datetime
from flask import flash, render_template, redirect, url_for
from sqlalchemy.exc import SQLAlchemyError
from . import main_blueprint
from .forms import AddContactForm, RegisterForm, SendMessageForm
from ...decorators import register_required
from ...models import db, ContactModel, MessageModel, UserModel
from ...utils import flash_errors
from ....service import ClientService
from .....server import get_hash, TrackerService, UserData


@main_blueprint.route('/', methods=['GET'])
@register_required
def index():
    contacts = ContactModel.query.filter(ContactModel.messages).all()
    messages = [contact.messages.order_by(MessageModel.time.desc()).first() for contact in contacts]
    content = zip(contacts, messages)
    return render_template('index.html', content=content)


@main_blueprint.route('/register', methods=['GET', 'POST'])
def register():
    form = RegisterForm()
    if form.validate_on_submit():
        phone = form.phone.data
        name = form.name.data
        user = UserModel(phone, name)
        result = ClientService.store_user_data(phone, 0, name, '1234', TrackerService.get_ip(), 3000)
        if not result:
            flash('Network access not available')
            return render_template('register.html', form=form)
        try:
            db.session.add(user)
            db.session.commit()
        except SQLAlchemyError:
            db.session.rollback()
            flash('Error')
            return render_template('register.html', form=form)
        return redirect(url_for('main.index'))
    else:
        flash_errors(form)
    return render_template('register.html', form=form)


@main_blueprint.route('/contacts', methods=['GET'])
@register_required
def contacts():
    contacts = ContactModel.query.all()
    return render_template('contacts.html', contacts=contacts)


@main_blueprint.route('/add_contact', methods=['GET', 'POST'])
@register_required
def add_contact():
    form = AddContactForm()
    if form.validate_on_submit():
        phone = form.phone.data
        contact_id = get_hash(f'{phone}:0')
        result = ClientService.get_user_data(contact_id)
        if not result:
            flash('Network access not available')
            return render_template('add_contact.html', form=form)
        user_data = UserData.from_json(result)
        contact = ContactModel(user_data.get_id(), user_data.get_phone(), user_data.get_name(), *user_data.get_dir())
        try:
            db.session.add(contact)
            db.session.commit()
        except SQLAlchemyError:
            db.session.rollback()
            flash('Error')
            return render_template('add_contact.html', form=form)
        return redirect(url_for('main.contacts'))
    else:
        flash_errors(form)
    return render_template('add_contact.html', form=form)


@main_blueprint.route('/chat/<contact_id>', methods=['GET', 'POST'])
@register_required
def chat(contact_id):
    form = SendMessageForm()
    contact = ContactModel.query.get_or_404(contact_id)
    messages = contact.messages.order_by(MessageModel.time.desc()).all()
    if form.validate_on_submit():
        text = form.text.data
        message = MessageModel(text, False, datetime.now())
        message.sender = contact
        result = ClientService.send_message_to(app, text, contact.tracker_id, contact.ip, contact.port, str(message.time))
        if not result:
            flash('Network access not available')
            form.text.data = text
            return render_template('chat.html', form=form, contact=contact, messages=messages)
        try:
            db.session.add(message)
            db.session.commit()
        except SQLAlchemyError:
            db.session.rollback()
            flash('Error')
            return render_template('chat.html', form=form, contact=contact, messages=messages)
        messages = contact.messages.order_by(MessageModel.time.desc()).all()
        form.text.data = ''
    else:
        flash_errors(form)
    return render_template('chat.html', form=form, contact=contact, messages=messages)


@main_blueprint.app_errorhandler(403)
def forbidden(e):
    return render_template('errors/403.html'), 403


@main_blueprint.app_errorhandler(404)
def page_not_found(e):
    return render_template('errors/404.html'), 404


@main_blueprint.app_errorhandler(500)
def internal_server_error(e):
    return render_template('errors/500.html'), 500


@main_blueprint.app_context_processor
def inject_current_user():
    return dict(current_user=UserModel.query.first)
