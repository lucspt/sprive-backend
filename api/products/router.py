from flask import Blueprint

bp = Blueprint("products", __name__)

import api.products.routes