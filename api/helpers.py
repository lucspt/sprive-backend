"""Helpers for flask views.

Wrappers and decorators for regular routes and auth-protected routes.
Common functions that are used for both partner and user endpoints.
"""

import json
from flask import make_response, Response
from typing import Callable, Literal, Iterable
from functools import wraps
from exceptions import (
    ExceptionWithStatusCode, 
    UnauthorizedError, 
    ResourceNotFoundError, 
    ResourceConflictError,
    InvalidMediaTypeError
)
from root.user import User 
from functools import wraps
from datetime import datetime, timedelta, timezone
import pandas as pd
from werkzeug.datastructures import FileStorage
from bson import ObjectId
from pymongo import MongoClient, errors as MongoErrors, collection, database
from root.partner import Partner, GHG_CATEGORIES_TO_UPLOAD_TASKS
from flask_jwt_extended import (
    create_access_token, 
    get_jwt, get_jwt_identity, 
    verify_jwt_in_request,
    set_access_cookies
)

def send(status: int, **kwargs) -> Response:
    """Json serialize a view with a status code
    
    Args:
        status (int): The status code to make the response with
        
    Returns:
        A flask Response
    """
    return make_response(json.dumps({**kwargs}, default=str), status)

def route(
    needs_db: bool = False, 
    send_return = True,
    success_code: int = 200,
) -> Callable:
    """Wraps a flask view
    
    Get a MongoClient instance and implement error handling 
    for a flask view. Whatever the view returns is json serialized by default
    
    Can be used with or without parameters
    
    Args:
        needs_db (bool): If true, pass a MongoClient to the `client` 
            argument of the view
        send_return (bool): Whether or not to json serialize the function result
        success_code (int): The status code to return when no error is raised
        
    Returns:
        The wrapped flask view function
        
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
            except ExceptionWithStatusCode as e:
                return send(content=e, error=e, status=e.status_code) 
            except Exception as e:
                return send(content=e, error=e, status=400)
            return send(
                content=response, status=success_code
            ) if send_return else response
        return _inner
    return _wrapper

def _refresh_partner_cookies_if_needed(
    response: Response,
    token_expiration: datetime.timestamp, 
    savior_id: str,
) -> None:
    """Refreshes a partners authorization cookies if needed.
    
    Only will refresh if the token they currently are requesting with
    expires in 30 minutes or less.
    
    Args:
        response (Response): The response to set cookies on.
        token_expiration (datetime.timestamp): The expiration timestamp on the jwt token.
        savior_id (str): The identity to create the token with, 
            and add as additional_claims to the token
        
    Returns:
        None
    """
    should_refresh = datetime.timestamp(
        datetime.now(tz=timezone.utc) + timedelta(minutes=30)
    )
    if should_refresh > token_expiration:
        refreshed_token = create_access_token(
            identity=savior_id,
            additional_claims={
                "savior_type": "partners",
                "partner": savior_id
            }
        )
        set_access_cookies(response, refreshed_token)
    return None
    

def savior_route(
    _func: Callable | None = None,
    *,
    send_return: bool = True,
    success_code: int = 200, 
) -> Callable:
    """Route decorator for auth-required flask views
    
    It will pass the correct `Savior` class to the view given the 
    auth token in the headers or cookies of the request. If the 
    view throws an exception this decorator will catch it and
    return it and it's status code (if present) as the response content. 
    It can be used with or without parameters.
    
    It also will refresh cookies when they expire if the request
    comes from a partner.
    
    Args:
        _func (Callable | None): The function to wrap when 
            decorating without invocating, i.e without parameters
        send_return (bool): Named arg. Whether or not to json
            serialize the response
        success_code (int): Named arg. The status code to make the Response with
            when successful
    
    Returns:
        The function it's wrapping, called with the relevant `Savior`
        class (User | Partner), as the first positional argument
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
                return send(content=e, error=e, status=401)
            try:
                if savior_type == "partners":
                    savior = Partner(savior_id=jwt["partner"], user_id=savior_id)              
                elif savior_type == "users":
                    savior = User(savior_id=savior_id)
                res = send(
                    content=func(savior, *args, **kwargs), status=success_code
                ) if send_return else func(savior, *args, **kwargs)
                savior._close()
            except ExceptionWithStatusCode as e:
                return send(content=e, error=e, status=e.status_code)
            except Exception as e:
                return send(content=e, error=e, status=400)
            if savior_type == "partners":
                _refresh_partner_cookies_if_needed(
                    response=res, 
                    token_expiration=jwt["exp"],
                    savior_id=savior_id
                )
            return res
        return _inner
    return _wrapper(_func) if _func else _wrapper

def _login(
    savior_id: str,
    savior_type: Literal["partners", "users"], 
    username: str,
    email: str,
    partner_token_identity: str | None = None,
    **response_kwargs, 
) -> dict:
    """Account login helper
    
    Create an access token and account response for a /login endpoint
    
    Args:
        savior_id (str): The _id of the account to login to
            if savior_type is partners, then 
            this should be the company_id of the partner
        savior_type (Literal[partners, users]): Whether we are
            loggin into a partner or user account
        username (str): The username of the account
        email (str): The account email
        partner_token_identity: This will be the _id
            of the partner account, aka the company user
    
    Returns:
        A dictionary of the account to return
    """
    if savior_type == "users":
        access_token = create_access_token(
            identity=savior_id, 
            additional_claims={"savior_type": savior_type},
            expires_delta=False
        )
        res = {
            "username": username, 
            "savior_id": savior_id,
            "savior_type": savior_type,
            "email": email,
            **response_kwargs
        }
    else: 
        access_token = create_access_token(
            identity=partner_token_identity, 
            additional_claims={"savior_type": savior_type, "partner": savior_id}
        )
        res = {
            "username": username, 
            "savior_id": savior_id,
            "savior_type": savior_type,
            "email": email,
            "user_id": partner_token_identity,
            **response_kwargs
        }
    return res, access_token


def create_account(
    db: database.Database,
    account: dict, 
    savior_type: Literal["users", "partners"], 
) -> Response:
    """Create an account for a partner or user
    
    Args:
        client (MongoClient): The MongoClient for db access
        account (dict): The account information to insert
        savior_type (Literal["users", "partners"]): Whether
            the account will be for a user or partner

    Returns:
        A Response with the account dict If savior_type is
        partners access token cookies are set otherwise the
        account dictionary will contain the access_token
        
    Raises:
        DuplicateKeyError: When the requested accout email is 
            already in use
    """
    now = datetime.now(tz=timezone.utc)
    account["joined"] = now 
    collection = db[savior_type] 
    email = account["email"]
    email_error = None
    try:
        if check_email_availability(
            collection, email=account["email"], email_field="company_email"
        )["is_available"]:
            _id = str(collection.insert_one(account).inserted_id)
        else:
            email_error = "That email is already in use"
            raise ResourceConflictError(email_error)
    except ResourceConflictError as e:
        return send(content=e, status=e.status_code)
    except MongoErrors.DuplicateKeyError:
        return send(content="That username is already in use", status=409)
    account.pop("password")
    res, token = _login(
        _id, 
        savior_type, 
        username = account["username"],
        email = email,
        partner_token_identity=_id
    )
    if savior_type == "partners":
        tasks_savior_id = ObjectId(_id)
        tasks = [
                    {
                        **GHG_CATEGORIES_TO_UPLOAD_TASKS[ghg_category],
                        "savior_id": tasks_savior_id,
                        "created_at": now
                    }
                    for ghg_category in account["measurement_categories"]
                ]
        if tasks:
            db.tasks.insert_many(tasks)
        res = send(content=account, status=201)
        set_access_cookies(res, token)
    else:
        res["token"] = token
        res = send(content=res, status=201)
    return res

def login(
    collection: collection.Collection, 
    savior_type: Literal["users", "partners"],
    username_or_email: str,
    password: str,
) -> Response:
    """Login a savior
    
    Args:
        collection (Collection): The collection find the account from
        savior_type (Litera['users', 'partners']): Whether the account 
            is for a savior or partner
        username_or_email (str): The username, if account is for a user
            or email, if the account is for a partner to login to
        password (str): The password to login with
        
    Returns:
        A Response containing the savior's account
    
    Raises:
        Exception: When an account was not found given the information provided
    """
    if savior_type == "users":
        query_field = "username"
    else:
        query_field = "email"
    account = collection.find_one(
        {query_field: username_or_email}
    )
    if not account:
        raise ResourceNotFoundError(
            f"Could not find an account with that {query_field}"
        )
    elif password == account.pop("password"):
        if savior_type == "partners":
            token_id = str(account.pop("_id"))
            field = account.pop
            response, jwt = _login(
                savior_id=str(account["company_id"]), 
                savior_type=savior_type, 
                username=field("username"), 
                email=field("email"),
                partner_token_identity=token_id,
                **account,
            )
            response = send(content=response, status=200)
            set_access_cookies(response, jwt)
        elif savior_type == "users":
            response, jwt = _login(
                savior_id=str(account["_id"]), 
                savior_type=savior_type, 
                username=account["username"], 
                email=account["email"],
                current_pledge=account.get("current_pledge", {}),
                spriving=account.get("spriving", False)
            )
            response["token"] = jwt
            response = send(content=response, status=200)
    else:
        raise UnauthorizedError(f"The {query_field} and password do not match")
    return response 

def check_email_availability(
    collection: collection.Collection, 
    email: str, 
    email_field: Literal["company_email", "email"] = "email"
) -> dict:
    """Check whether an email is available for account creation
    
    Args:
        collection (Collection): The collection to search emails from
        email (str): The email to search for
        email_field Literal["company_email", "email"]: The email field to query
            will be company_email, for example when inviting a company user 
            otherwise email
            
    Returns:
        a dict with boolean field is_available
    """
    email_exists = collection.find_one({email_field: email}, {"_id": 1}) or {}
    return { "is_available": not bool(email_exists) }
    
import pandas as pd

def file_to_df(file: FileStorage, filename: str | None = None) -> pd.DataFrame:
    filename = filename or file.filename
    file_extension = filename.partition(".")[-1]
    if file_extension == "csv":
        return pd.read_csv(file)
    elif file_extension in ["xls", "xlsx"]:
        return pd.read_excel(file)
    else:
        raise InvalidMediaTypeError("Invalid file type")
    