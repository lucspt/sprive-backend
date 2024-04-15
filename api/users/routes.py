"""/users routes

Endpoints for logging in as a user, and creating users
"""

from api.users.router import bp
from api.helpers import (
    route, login, create_account, check_email_availability, send
)
from pymongo import MongoClient
from flask import request, Response

@bp.post("/login")
@route(needs_db=True, send_return=False)
def user_login(client: MongoClient) -> Response:
    """POST method to /users/login
    
    Expected json:
        username (str): The user's username
        password (str): The user's password
        
    Returns:
        A `Response` containing the user's account if the login was successful
        else 401 UNAUTHORIZED Response
    """
    user = request.json
    return login(
        collection=client.spt.users,
        savior_type="users",
        username_or_email=user["username"],
        password=user["password"]
    )

@bp.post("/", strict_slashes=False)
@route(needs_db=True, send_return=False, success_code=201)
def create_user(client: MongoClient) -> Response:
    """POST method to /users
    
    Create a user
    
    Expected json:
        username (str): The account's username
        password (str): The account's password
        email (str): The account's email
    Returns:
        A Response with the user's account or a Response with the status 
        code of 409 if a pymongo DuplicateKeyError is raised 
        when trying to create the account
    """
    user = request.json 
    return create_account(
        db=client.spt, 
        savior_type="users",
        account={
            "username": user["username"],
            "email": user["email"],
            "password": user["password"],
        },
    )

@bp.get("/emails/<string:email>")
@route(needs_db=True)
def uniquify_emails(client: MongoClient, email: str) -> dict[str, bool]:
    """GET method for /users/emails/<email>
    
    Check whether an email is available
    
    Path args:
        email (str): The email to check availability for
    Returns:
        A dict with the field is_available and its boolean key
    """
    return check_email_availability(collection=client.spt.users, email=email)
    
