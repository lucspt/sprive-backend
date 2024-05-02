from pytest import fixture
import pytest
from flask import Response
from bson import ObjectId
from tests.utils import create_account_test, decode_response, assert_route_successful
    
USERNAME = "LIFEBEAUTIFUL"

@pytest.mark.parametrize(
    ("duplicate_error"),
    [(True), (False)]
)
def test_create_account(
    duplicate_error: bool, 
    api, 
    mock_partner_account: dict, 
    db
):
    dupe_account = mock_partner_account.copy()
    if duplicate_error:
        dupe_account.pop("_id")
        dupe_account.pop("company_id")
        dupe_account["company_name"] = dupe_account.pop("company")
        
    res = create_account_test(
        endpoint="/partners", 
        duplicate_error=duplicate_error,
        duplicate_account=dupe_account,
        api=api,
        username_to_delete=USERNAME,
        extra_account_data={"measurement_categories": ["3.1"]}
    )
    if not duplicate_error: 
        savior_id = res["_id"]
        assert res["company_id"] == savior_id
        savior_id = ObjectId(savior_id)
        assert list(db.tasks.find({"savior_id": savior_id}))
        db.tasks.delete_many({"savior_id": savior_id})
        
        
@pytest.mark.parametrize(
    ("fail_email", "fail_password", "login_mock_partner_account"),
    [
        (False, False, True),
        (True, False, False),
        (False, True, False)
    ]
)
def test_login(
    fail_email: bool,
    fail_password: bool,
    login_mock_partner_account: bool, 
    mock_partner_account,
    api,
):
    if login_mock_partner_account:
        email, password = mock_partner_account["email"], mock_partner_account["password"]
        res = api.post("/partners/login", json={"email": email, "password": password})
        assert res.status_code == 200
        res = decode_response(res)["content"]
        assert mock_partner_account.keys() & res.keys()
    else:
        if fail_email:
            email = "fail@fail.com"
            password = mock_partner_account["password"]
            _assert = lambda res: res.status_code == 404
        elif fail_password:
            email = mock_partner_account["email"]
            password = "fail"
            _assert = lambda res: res.status_code == 401
        res = api.post("/partners/login", json={"email": email, "password": password})
        assert _assert(res)
        
def test_check_email_is_available(
    mock_partner_account: dict, 
    api, 
):
    def assert_available(email, expected):
        res = decode_response(api.get(f"/partners/emails/{email}"))
        assert res["content"]["is_available"] == expected
    assert_available(mock_partner_account["company_email"], False)
    assert_available("NONONUSESESTHISEMAIL@sprive.com", True)
    
NUM_PRODUCTS = 3
@fixture
def create_mock_products(db, mock_partner_account):
    delete_id = ObjectId()
    db.emission_factors.insert_many([
        {
            "savior_id": mock_partner_account["_id"], 
            "co2e": 123,
            "name": "mock",
            "delete_id": delete_id
        }
        for _ in range(NUM_PRODUCTS) 
    ])
    yield
    db.emission_factors.delete_many({"delete_id": delete_id})

@pytest.mark.usefixtures("create_mock_products")
def test_get_partner_endpoint(api, mock_partner_account, db):
    """Here we test / and /<string:partner_id> flask views"""
    mock_partner: Response = api.get(f"/partners/{mock_partner_account["_id"]}")
    status_code = mock_partner.status_code
    assert status_code < 300 and status_code >= 200
    parnter_card = decode_response(mock_partner)["content"]
    assert len(parnter_card["products"]) == NUM_PRODUCTS
    assert parnter_card["name"] == mock_partner_account["company"]
    for k in ["bio", "region"]:
        assert parnter_card[k] == mock_partner_account[k]
        
def test_partners_get_endpoint(api):
    assert_route_successful(api.get("/partners"), list)
     
@fixture(scope="module", autouse=True)
def cleanup(db):
    yield
    db.partners.delete_one({"username": USERNAME})
