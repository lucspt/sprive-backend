"""/pytesting routes

Bluepring routes which are only registered if the 
testing argument to `create_app` function is `True`
"""

from api.testing.router import bp
from api.helpers import savior_route
from flask import request
from root.partner import Partner
from root.user import User

@bp.get("/mock-jwt-required")
@savior_route
def mock_jwt_required(savior: Partner | User) -> str:
    """GET method for /mock-jwt-required.
    
    Simulate authorization requirements of the `savior_route` decorator.
    Also test that the correct savior is passed to the wrapped function
    
    Returns:
        "SUCCESS", if authorized, else a 401 UNAUTHORIZED `Response`
    """
    savior_type = request.args.get("savior_type")
    if savior_type == "users":
        assert isinstance(savior, User)
    elif savior_type == "partners":
        assert isinstance(savior, Partner)
    return "SUCCESS"
