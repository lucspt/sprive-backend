"""Common testing fixtures.

Note that the testing strategy is to extensively test `Savior` classes
and their returns,  and from there to assert that routes are fully,
i.e. requiring auth when expected and returning the correct dtypes.

Most routes will use `Savior` class methods, so testing those classes
will give confidence in routes as well. 


Notes: 
    When testing and wanting to insert to collections, either use a cleanup 
    fixture to clean up inserts, or you can just use the _id field of one 
    of `mock_user_account` or `mock_partner_account` fixtures

    When accessing the fixutres `partner_auth` and `user_auth` at the same time,
    i.e. in the same test / function, it can cause unexpected conditions and 
    raise errors. To avoid this, simply separate the user and partner tests
    into different functions.
"""

from tests.utils import decode_response, assert_route as route_assertion
from typing import Type, Callable, Any, Literal
from datetime import datetime, timezone
from pymongo import MongoClient
from typing import Generator
from app import create_app
from pytest import fixture
from flask import Flask
import pymongo.database
from bson import ObjectId
from pathlib import Path
import pandas as pd
import json

@fixture(scope="module")
def flask_app() -> Flask:
    """The testing Flask instance"""
    app = create_app(testing=True)
    app.config.update({
        "TESTING": True
    })
    return app
    
@fixture(scope="module")
def api(flask_app: Flask) -> Flask.test_client:
    """The flask test client keeps track of cookies during testing"""
    return flask_app.test_client(use_cookies=True)

@fixture(scope="session")
def db() -> Generator[pymongo.database.Database, None, None]:
    """The pymongo db with all collections"""
    client = MongoClient()
    yield client.spt 
    client.close()
 
 
@fixture(scope="session")
def mock_partner_account() -> dict[str, Any]:
    """A mock partner account.
    
    All test insertions using this account's _id or company_id
    fields are automatically deleted after the test session
    """
    _id = ObjectId()
    return {
            '_id': _id,
            'company': 'lovelife',
            'password': 'test',
            'email': 'test@test.com',
            'company_email': 'test@test.com',
            'company_id': _id,
            'role': 'company',
            'region': 'US',
            'joined': datetime.now(tz=timezone.utc),
            'username': "test_username",
            'bio': 'We love life!',
            'team': None,
            "measurement_categories": ["3.1"],
        }
    
@fixture(scope="session")
def mock_user_account() -> dict[str, Any]:
    """Mock user account.
    
    Any inserts using this account's _id are deleted after tesing
    """
    return {
       "_id": ObjectId(),
        'username': 'SPRIVEBACKENDTESTING',
        'joined': datetime.now(tz=timezone.utc),
        'email': 'test@gmailll.com',
        'password': 'test',
        'current_pledge': {'frequency': 'day', 'co2e': 12, 'message': 'We love life'},
        'spriving': True
    }

@fixture(scope="session", autouse=True)
def insert_saviors(
    db: pymongo.database.Database, mock_partner_account: dict, mock_user_account: dict
) -> Generator[None, None, None]:
    """Insert the mock savior accounts"""
    partner_id = db.partners.insert_one(mock_partner_account).inserted_id
    user_id = db.users.insert_one(mock_user_account).inserted_id
    yield
    db.partners.delete_one({"_id": partner_id})
    db.users.delete_one({"_id": user_id})
   
@fixture
def partner_auth(
    api: Flask.test_client, mock_partner_account: dict
) -> Generator[dict, None, None]:
    """The mock partner account's authorization for making mock requests.
    
    Logs in to the account every time this fixture is used so that cookies are set.
    Logs out after fixture usage.
    
    See module docstring Note before usage.
    """
    api.post("/partners/login", json={
        "email": mock_partner_account["email"],
        "password": mock_partner_account["password"]
    })
    yield {
        "X-CSRF_TOKEN": api.get_cookie("csrf_access_token").value
    }   
    api.delete("/saviors/logout")

@fixture
def user_auth(
    mock_user_account: dict, api: Flask.test_client
) -> Generator[dict, None, None]:
    """The mock user account's authorization for making mock requests.
    
    Logs in to the account every time this fixture is used.
    Logs out after fixture usage.
    
    See module docstring Note before usage.
    """
    res = api.post("/users/login", json={
        "username": mock_user_account["username"],
        "password": mock_user_account["password"]
    })
    res = decode_response(res)["content"]
    yield {
        "Authorization": f'Bearer {res["token"]}',   
    }
    api.delete("/saviors/logout")

@fixture
def assert_route(api) -> Callable[[str, str, dict, Type | tuple[Type, ...], dict], Any]:
    def _assert(
       endpoint: str, 
       method: Literal["get", "post", "put", "patch", "delete"], 
       auth: dict, 
       expect: Type | tuple[Type, ...] = Any, 
       headers: dict={}, 
       **request
    ) -> Any:
        """Helper to assert a route that requires auth is functioning correctly
        
        See `utils.assert_route` for more
        
        Args:
            endpoint (str): The endpoint to assert
            method (str): The method to test
            auth (dict): The authorization headers to test
            expect (Type | tuple[Type, ...]): The expected Response dtype to assert
            headers (dict): Any headers to add to the request along with `auth`.
        """
        _json = request.pop("json", None)
        if _json:
            # because some objects will throw errors in a json request, i.e ObjectId
            request["json"] = json.loads(json.dumps(_json, default=str))
        return route_assertion(
            api_call=getattr(api, method),
            endpoint=endpoint,
            auth_headers=auth,
            headers=headers,
            expected_instance=expect,
            **request,
        )
    return _assert

@fixture
def create_file(tmp_path: Path) -> Callable[[str, list[dict]], tuple[bytes, str]]:
    """Write and open a csv-like file to a temporary `Path` for mocking POST requests.
    
    Args: 
        filename (str): The filename to return with the data.
        data (list | pd.DataFrame): The records or csv data to write to the file.
    
    Returns:
        A tuple with the bytes buffer and filename to send in a request.
    """
    def _create(filename: str, data: list[dict] | pd.DataFrame):
        write_to = tmp_path / filename
        if isinstance(data, list):
            data = pd.DataFrame().from_records(data)
        data.to_csv(write_to, index=False)
        return (open(write_to, "rb"), filename)
    return _create

@fixture(scope="session")
def mock_product_csv() -> pd.DataFrame:
    """A csv with valid data mocking a product creation file"""
    return pd.read_csv("../data/mock-product.csv")
        
@fixture(scope="session", autouse=True)
def teardown(
    db: pymongo.database.Database,
    mock_partner_account: dict,
    mock_user_account: dict
):
    """Delete all db inserts caused by testing"""
    yield
    delete_ids = [mock_user_account["_id"], mock_partner_account["_id"]]
    for collection_name in db.list_collection_names():
        db[collection_name].delete_many({
            "$or": [
                {"savior_id": {"$in": delete_ids}},
                {"_id": {"$in": delete_ids}},
                {"company_id": {"$in": delete_ids}},
            ]
        })