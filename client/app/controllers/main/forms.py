from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, TextAreaField
from wtforms.validators import DataRequired


class RegisterForm(FlaskForm):
    phone = StringField('Phone', validators=[DataRequired()])
    name = StringField('Name', validators=[DataRequired()])
    submit = SubmitField('Submit')


class AddContactForm(FlaskForm):
    phone = StringField('Phone', validators=[DataRequired()])
    submit = SubmitField('Submit')


class SendMessageForm(FlaskForm):
    text = TextAreaField('Text', validators=[DataRequired()])
    submit = SubmitField('Send')
