from flask import Blueprint

bp = Blueprint("saviors", __name__)

import api.saviors.routes
import api.saviors.user_routes
import api.saviors.partner_routes
