from flask import make_response, Response
from typing import Callable, Type, Literal
from functools import wraps
from pymongo import MongoClient, errors as MongoErrors, collection
import json
from typing import Callable, Type
from flask_jwt_extended import (
    create_access_token, 
    get_jwt, get_jwt_identity, 
    verify_jwt_in_request,
    set_access_cookies
)
from root.saviors.partner import Partner
from root.saviors.user import User 
from functools import wraps
from datetime import datetime, timedelta, timezone
import pandas as pd
from werkzeug.datastructures import FileStorage

def send(status: int, **kwargs) -> Response:
    """Json serialize a response with Response status code"""
    return make_response(json.dumps({**kwargs}, default=str), status)

def route(
    needs_db: bool = False, 
    send_return = True,
    error_check: Type[Exception] = Exception,
    sucess_code: int = 200, 
    error_code: int = 400
) -> Callable:
    """Wraps a route with error handling and json serializing.
    
    It also give access to db
    """
    
    def _wrapper(func: Callable):
        @wraps(func)
        def _inner(*args, **kwargs):
            try:
                if needs_db:
                    client = MongoClient()
                    response = func(client=client, *args, **kwargs)
                    client.close()
                else:
                    response = func(*args, **kwargs)
            except error_check as e:
                return send(
                    content=e, status=error_code
                )
            return send(
                content=response, status=sucess_code
            ) if send_return else response
        return _inner
    return _wrapper

def savior_route(
    _func=None,
    *,
    send_return: bool = True,
    error_check: Type[Exception] = Exception,
    error_message: str | None = None,
    sucess_code: int = 200, 
    error_code: int = 400,
) -> Callable:
    """this wraps a route, with the same functionality as `route`,
    
    it also:
    
        - verifies the jwt payload
        - passes an appropriate `Savior` class to the function 
        - refreshes token if needed
    """
    @wraps(_func)
    def _wrapper(func: Callable | None = _func) -> Callable:
        @wraps(func)
        def _inner(*args, **kwargs):
            try: 
                verify_jwt_in_request()
                savior_id = get_jwt_identity()
                jwt = get_jwt()
                savior_type = jwt["savior_type"]
            except Exception as e:
                return send(content=e, status=401)
            try:
                if savior_type == "partners":
                    savior = Partner(savior_id=savior_id)
                elif savior_type == "users":
                    savior = User(savior_id=savior_id)
                res = send(
                    content=func(savior, *args, **kwargs), status=sucess_code
                ) if send_return else func(savior, *args, **kwargs)
                savior._close()
            except error_check as e:
                return send(content=error_message or e, status=error_code)
            if savior_type == "partners":
                token_expiration = jwt["exp"]
                should_refresh = datetime.timestamp(
                    datetime.now(tz=timezone.utc) + timedelta(minutes=30)
                )
                if should_refresh > token_expiration:
                    refreshed_token = create_access_token(
                        identity=savior_id,
                        additional_claims={"savior_type": savior_type}
                    )
                    set_access_cookies(res, refreshed_token)
            return res
        return _inner
    return _wrapper(_func) if _func else _wrapper

def _login(
    savior_id: str, 
    savior_type: Literal["partners", "users"], 
    username: str,
    email: str,
) -> dict:
    """Create a jwt and format a response when logging in and signing up"""
    if savior_type == "users":
        access_token = create_access_token(
            identity=savior_id, 
            additional_claims={"savior_type": savior_type},
            expires_delta=False
        )
    else: 
        access_token = create_access_token(
            identity=savior_id, 
            additional_claims={"savior_type": savior_type}
        )
    return {
        "username": username, 
        "savior_id": savior_id,
        "savior_type": savior_type,
        "email": email
    }, access_token
    

def create_account(
    client: MongoClient, 
    savior_type: Literal["users", "partners"], 
    username: str,
    password: str,
    email: str,
    include_token: bool,
    **kwargs,
) -> dict:
    """Create an account, insert into db and login savior,
    
    if a partner account was created there will be a task
    inserted into db for them to upload their first file for calculation \n
    """
    now = datetime.now(tz=timezone.utc)
    account = {
        "username": username, 
        "joined": now,
        "email": email,
        "password": password,
        "current_pledge": {},
        "spriving": False,
        **kwargs
    }
    db = client.spt 
    collection = db[savior_type] 
    try:
        _id = collection.insert_one(
            account
        ).inserted_id
    except MongoErrors.DuplicateKeyError:
        return send(
            content="That username is already in use", 
            status=409
        )
    res, token = _login(str(_id), savior_type, username, email)
    if include_token:
        res["token"] = token
    if savior_type == "partners":
        db.tasks.insert_one({
            "created": now, 
            "name": "first task",
            "category": "data",
            "description": "upload your first file and calculate emissions to start your journey",
            "status": "in progress",
            "savior_id": _id,
            "assignees": []
        })
        res = send(content=res, status=200)
        set_access_cookies(res, token)
    else:
        res = send(content=res, status=200)
    return res
def login(
    collection: collection.Collection, 
    savior_type: Literal["users", "partners"],
    username: str,
    password: str,
    include_token: bool,
) -> Response:
    """Login a savior.
    
    When the query param token = include it returns jwt for Bearer (headers) usage
    """
    account = collection.find_one(
        {"username": username}
    )
    if not account:
        raise Exception("Could not find an account with that username")
    elif password == account["password"]:
        response, jwt = _login(
            str(account["_id"]), savior_type, username, email=account["email"]
        )
        response["current_pledge"] = account.get("current_pledge", {})
        response["spriving"] = account.get("spriving", False)
    else: 
        raise Exception("The password and username do not match")
    if include_token:
        response["token"] = jwt
        response = send(content=response, status=200)
    else:
        response = send(content=response, status=200)
        set_access_cookies(response, jwt)
    return response 

def check_email_availability(collection: collection.Collection, email: str) -> dict:
    """This is a helper function for endpoints that will recieve an email 
    from a two step form during account creation and checks if it is already in use
    """
    email_exists = collection.find_one(
        {"email": email}
    ) or {}
    return {
        **email_exists,
        "is_available": not bool(email_exists)
    }
    
import pandas as pd

def file_to_df(file: FileStorage, filename: str | None = None) -> pd.DataFrame:
    filename = filename or file.filename
    file_extension = filename.partition(".")[-1]
    if file_extension == "csv":
        return pd.read_csv(file)
    elif file_extension in ["xls", "xlsx"]:
        return pd.read_excel(file)
    else:
        raise Exception("Invalid file type")
    