import pytest 
from pytest import fixture
from tests.utils import (
    decode_response, 
    create_account_test, 
    assert_route_successful, 
    assert_route_unauthorized
)

@fixture(scope="module")
def test_account_email():
    return "test-user@gmail.com"

USERNAME = "LIFELOVE"
@fixture(scope="module", autouse=True)
def delete_user(db):
    db.users.delete_one({"username": USERNAME})
    
def test_user_login(mock_user_account, api):
    assert_route_successful(
        api.post(
            "/users/login",
            json={
                "username": mock_user_account["username"],
                "password": mock_user_account["password"]
            }
        ),
        dict
    )
    assert_route_unauthorized(
        api.post(
            "/users/login",
            json={
                "username": "",
                "password": ""
            }
        )
    )
    
@pytest.mark.parametrize(
    ("duplicate_error"),
    [(True), (False)]
)
def test_create_account(
    duplicate_error: bool, 
    api, 
    mock_user_account: dict, 
):
    dupe_account = mock_user_account.copy()
    if duplicate_error:
        dupe_account.pop("_id")
        
    res = create_account_test(
        endpoint="/users", 
        duplicate_error=duplicate_error,
        duplicate_account=dupe_account,
        api=api,
        username_to_delete=USERNAME,
    )
    if not duplicate_error: 
        assert res["token"]
        
        
@fixture
def test_emails(db):
    email = "mock_inavailable"
    db.users.insert_one({"email": email})
    yield {
        email: False,
        "mock_available": True
    }
    db.users.delete_one({"email": email})

def test_emails_get(api, test_emails):
    for email, res_should_be in test_emails.items():
        res = api.get(f"/users/emails/{email}")
        assert_route_successful(res, dict)
        res = decode_response(res)["content"]
        assert res["is_available"] == res_should_be