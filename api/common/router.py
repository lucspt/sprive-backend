from flask import Blueprint

bp = Blueprint("common", __name__)

import api.common.routes