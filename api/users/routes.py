from api.users.router import bp
from api.helpers import (
    savior_route, route, login, create_account, check_email_availability, send
)
from flask_jwt_extended import (
    jwt_required, get_jwt_identity, create_access_token, create_refresh_token
)
from root.saviors.user import User
from pymongo import MongoClient
from flask import request 

@bp.post("/login")
@route(needs_db=True, send_return=False)
def user_login(client: MongoClient):
    """Login a user"""
    user = request.json
    return login(
        collection=client.spt.users,
        savior_type="users",
        include_token=True,
        **user
    )

@bp.post("/", strict_slashes=False)
@route(needs_db=True, send_return=False)
def create_user(client: MongoClient):
    """Create a new user account"""
    user = request.json 
    username, email, password = user["username"], user["email"], user["password"]
    return create_account(
        client=client, 
        savior_type="users",
        username=username,
        password=password,
        email=email,
        include_token=request.args.get("token") == "include"
    )

@bp.get("/emails/<string:email>")
@route(needs_db=True)
def uniquify_emails(client: MongoClient, email: str) -> dict:
    """This endpoint will recieve an email 
    from a two step form that handles account creation.
    It checks if the email is already in use"""
    return check_email_availability(collection=client.spt.users, email=email)
    
