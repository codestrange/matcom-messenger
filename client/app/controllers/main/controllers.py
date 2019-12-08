from datetime import datetime
from logging import error
from random import randint
from flask import current_app, flash, render_template, redirect, url_for
from sqlalchemy.exc import SQLAlchemyError
from . import main_blueprint
from .forms import AddContactForm, AddGroupForm, CreateGroupForm, RegisterForm, SendMessageForm
from ...decorators import register_required
from ...models import db, ContactModel, GroupModel, MessageModel, UserModel
from ...utils import flash_errors
from ....service import ClientService
from .....server import get_hash, TrackerService, UserData


@main_blueprint.route('/', methods=['GET'])
@register_required
def index():
    contacts = ContactModel.query.filter(ContactModel.messages).all()
    contact_messages = [
        contact.messages.filter_by(group=None).order_by(MessageModel.time.desc()).first()
        for contact in contacts
    ]
    groups = GroupModel.query.filter(GroupModel.messages).all()
    group_messages = [
        group.messages.order_by(MessageModel.time.desc()).first()
        for group in groups
    ]
    messages = contact_messages + group_messages
    messages.sort(key=lambda x: x.time)
    return render_template('index.html', messages=messages)


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
        except SQLAlchemyError as e:
            error(f'register - {e}')
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


@main_blueprint.route('/groups', methods=['GET'])
@register_required
def groups():
    groups = GroupModel.query.all()
    return render_template('groups.html', groups=groups)


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
        contact = ContactModel(user_data.get_id(), user_data.get_phone(), user_data.get_name()[0], *user_data.get_dir()[0])
        try:
            db.session.add(contact)
            db.session.commit()
        except SQLAlchemyError as e:
            error(f'add_contact - {e}')
            db.session.rollback()
            flash('Error')
            return render_template('add_contact.html', form=form)
        return redirect(url_for('main.contacts'))
    else:
        flash_errors(form)
    return render_template('add_contact.html', form=form)


@main_blueprint.route('/add_group', methods=['GET', 'POST'])
@register_required
def add_group():
    form = AddGroupForm()
    if form.validate_on_submit():
        user = UserModel.query.first()
        group_id = form.id.data
        result = ClientService.add_user_to_group(current_app, user.tracker_id, group_id)
        if not result:
            flash('Network access not available')
            return render_template('add_group.html', form=form)
        result = ClientService.get_user_data(group_id)
        if not result:
            flash('Network access not available')
            return render_template('add_group.html', form=form)
        user_data = UserData.from_json(result)
        group = GroupModel(user_data.get_id(), user_data.get_name()[0])
        try:
            db.session.add(group)
            db.session.commit()
        except SQLAlchemyError as e:
            error(f'add_contact - {e}')
            db.session.rollback()
            flash('Error')
            return render_template('add_group.html', form=form)
        return redirect(url_for('main.groups'))
    else:
        flash_errors(form)
    return render_template('add_group.html', form=form)


@main_blueprint.route('/remove_group/<group_id>', methods=['GET'])
@register_required
def remove_group(group_id):
    user = UserModel.query.first()
    group = GroupModel.query.get_or_404(group_id)
    result = ClientService.remove_user_from_group(current_app, user.tracker_id, group.tracker_id)
    if not result:
        flash('Network access not available')
        return redirect(url_for('main.chat_public', group_id=group_id))
    try:
        db.session.delete(group)
        db.session.commit()
    except SQLAlchemyError as e:
        error(f'add_contact - {e}')
        db.session.rollback()
        flash('Error')
        return redirect(url_for('main.chat_public', group_id=group_id))
    return redirect(url_for('main.groups'))


@main_blueprint.route('/create_group', methods=['GET', 'POST'])
@register_required
def create_group():
    form = CreateGroupForm()
    if form.validate_on_submit():
        name = form.name.data
        user = UserModel.query.first()
        nonce = randint(1, 10 ** 12)
        tracker_id = get_hash(f'{user.phone}:{nonce}')
        group = GroupModel(tracker_id, name)
        result = ClientService.store_user_data(user.phone, nonce, name, '1234', TrackerService.get_ip(), 3000)
        if not result:
            flash('Network access not available')
            return render_template('create_group.html', form=form)
        try:
            db.session.add(group)
            db.session.commit()
        except SQLAlchemyError as e:
            error(f'register - {e}')
            db.session.rollback()
            flash('Error')
            return render_template('create_group.html', form=form)
        return redirect(url_for('main.groups'))
    else:
        flash_errors(form)
    return render_template('create_group.html', form=form)


@main_blueprint.route('/chat_private/<contact_id>', methods=['GET', 'POST'])
@register_required
def chat_private(contact_id):
    form = SendMessageForm()
    user = UserModel.query.first()
    contact = ContactModel.query.get_or_404(contact_id)
    messages = contact.messages.filter_by(group=None).order_by(MessageModel.time.desc()).all()
    if form.validate_on_submit():
        text = form.text.data
        message = MessageModel(text, False, datetime.now())
        message.sender = contact
        result = ClientService.send_message_to(current_app, text, user.tracker_id, contact.tracker_id, contact.ip, contact.port, str(message.time))
        if not result:
            flash('Network access not available')
            form.text.data = text
            return render_template('chat_private.html', form=form, contact=contact, messages=messages)
        try:
            db.session.add(message)
            db.session.commit()
        except SQLAlchemyError as e:
            error(f'chat_private - {e}')
            db.session.rollback()
            flash('Error')
            return render_template('chat_private.html', form=form, contact=contact, messages=messages)
        messages = contact.messages.filter_by(group=None).order_by(MessageModel.time.desc()).all()
        form.text.data = ''
    else:
        flash_errors(form)
    return render_template('chat_private.html', form=form, contact=contact, messages=messages)


@main_blueprint.route('/chat_public/<group_id>', methods=['GET', 'POST'])
@register_required
def chat_public(group_id):
    form = SendMessageForm()
    user = UserModel.query.first()
    group = GroupModel.query.get_or_404(group_id)
    messages = group.messages.order_by(MessageModel.time.desc()).all()
    if form.validate_on_submit():
        text = form.text.data
        message = MessageModel(text, False, datetime.now())
        message.group = group
        result = ClientService.send_message_to_group(current_app, text, user.tracker_id, group.tracker_id, str(message.time))
        if not result:
            flash('Network access not available')
            form.text.data = text
            return render_template('chat_public.html', form=form, group=group, messages=messages)
        try:
            db.session.add(message)
            db.session.commit()
        except SQLAlchemyError as e:
            error(f'chat_public - {e}')
            db.session.rollback()
            flash('Error')
            return render_template('chat_public.html', form=form, group=group, messages=messages)
        messages = group.messages.order_by(MessageModel.time.desc()).all()
        form.text.data = ''
    else:
        flash_errors(form)
    return render_template('chat_public.html', form=form, group=group, messages=messages)


@main_blueprint.route('/profile', methods=['GET'])
def profile():
    return render_template('/profile.html')


@main_blueprint.route('/logout', methods=['GET'])
def logout():
    db.drop_all()
    db.create_all()
    return redirect(url_for('main.index'))


@main_blueprint.route('/reload_private/<contact_id>', methods=['GET'])
def reload_private(contact_id):
    return redirect(url_for('main.chat_private', contact_id=contact_id))


@main_blueprint.route('/reload_public/<group_id>', methods=['GET'])
def reload_public(group_id):
    return redirect(url_for('main.chat_public', group_id=group_id))


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
