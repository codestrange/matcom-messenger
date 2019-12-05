from flask import flash


def flash_errors(form):
    for field, errors in form.errors.items():
        for error in errors:
            flash(f'{error}')
