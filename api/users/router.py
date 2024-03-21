from flask import Blueprint

bp = Blueprint("users", __name__)

import api.users.routes