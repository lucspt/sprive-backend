from pytest import fixture
from bson import ObjectId
import pytest
from datetime import datetime, timezone, timedelta

PRODUCT_ID = ObjectId(b"123456789101")
CO2E = 10
@fixture(scope="function")
def insert_mock_product(db, mock_partner_account):
    mock_product = {
        "product_id": PRODUCT_ID,
        "savior_id": mock_partner_account["_id"],
        "name": "testing-lovelife",
        "co2e": CO2E,
        "source": "partners",
    }
    db.emission_factors.insert_one(mock_product)

@pytest.mark.usefixtures("insert_mock_product")
@pytest.mark.parametrize(
    ("value"), [1, 2]
)
def test_logs_post(value, assert_route, user_auth, db, mock_user_account, mock_partner_account):
    log = assert_route(
        "/saviors/logs", 
        "post", 
        user_auth,
        dict,
        json={
        "product_id": PRODUCT_ID,
        "value": value
    })
    db.logs.delete_many({"product_id": PRODUCT_ID})
    assert ObjectId(log["product_id"]) == PRODUCT_ID
    assert db.emission_factors.aggregate([
       {"$match":  {"product_id": PRODUCT_ID, "source": "partners", "savior_id": mock_partner_account["_id"]}},
       {"$project": {"co2e": {"$multiply": ["$co2e", value]},}}
    ]).next()["co2e"] == value * CO2E
    db.product_logs.delete_many(
        {"product_id": PRODUCT_ID, "savior_id": mock_user_account["_id"]}
    ) # had to delete so it doesn't interfere with other testing logic 

@pytest.mark.parametrize(
    ("limit", "skip"),
    [(0, 0), (10, 20)]
)
def test_stars_get(limit, skip, assert_route, user_auth):
    assert_route(
        f"/saviors/stars",
        "get",
        user_auth,
        dict,
        query_string={"limit": limit, "skip": skip}
    )
    
@pytest.mark.parametrize(
    ("since_date"),
    [
        (datetime.now(tz=timezone.utc).isoformat()), 
        ((datetime.now(tz=timezone.utc) - timedelta(days=10)).isoformat())
    ]
)
def test_times_logged_get(since_date, assert_route, user_auth):
    assert_route(
        "/saviors/times-logged",
        "get",
        user_auth,
        int,
        query_string={"since_date": since_date}
    )
    
def test_sprivers_post(assert_route, user_auth):
    assert_route(
        "/saviors/sprivers",
        "post",
        user_auth,
        bool,
    )
    
def test_sprivers_delete(assert_route, user_auth):
    assert_route(
        "/saviors/sprivers",
        "post",
        user_auth,
        bool,
    )
    
@fixture
def mock_product_id(db, mock_partner_account):
    mock_product_id = ObjectId()
    _id = db.emission_factors.insert_one({
        "savior_id": mock_partner_account["_id"], 
        "product_id": mock_product_id,
        "co2e": "123",
        "name": "mock"
    }).inserted_id
    yield mock_product_id
    db.emission_factors.delete_one({"_id": _id})
    
    
def test_star_product(api, assert_route, user_auth, mock_product_id, db):
    with pytest.raises(Exception):
        api.post("/saviors/stars/fail_product_id", user_auth)
    print(mock_product_id, "MOCK ID", db.emission_factors.find_one({"product_id": mock_product_id}))
    for method in ["post", "delete"]:
        assert_route(
            f"/saviors/stars/{mock_product_id}",
            method,
            user_auth,
            bool
        )
    
@pytest.mark.parametrize(
    ("frequency", "co2e", "method"),
    [("day", 10, "post"), ("week", 10, "put")]
)
def test_current_pledge_post_put(frequency, co2e, method, assert_route, user_auth):
    assert_route(
        "/saviors/current-pledge",
        method, 
        user_auth,
        bool,
        json={
            "frequency": frequency,
            "co2e": co2e,
        }
    )
def test_current_pledge_delete(assert_route, user_auth):
    assert_route(
        "/saviors/current-pledge",
        "delete",
        user_auth,
        bool
    )