from flask import Blueprint 

bp = Blueprint("factors", __name__)

import api.factors.routes