from flask import Blueprint

bp = Blueprint("partners", __name__)

import api.partners.routes