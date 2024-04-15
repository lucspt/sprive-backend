from flask import Blueprint

bp = Blueprint("tasks", __name__)

import api.tasks.routes