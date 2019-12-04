from flask import Blueprint

main_blueprint = Blueprint('main', __name__, template_folder='../../views')

from . import controllers
