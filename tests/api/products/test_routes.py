from pytest import fixture
from bson import ObjectId
from tests.utils import decode_response, assert_route_successful
from root.partner import Partner
import pytest

PRODUCT_NAME = "PRODUCT_ROUTES_TESTING"

@fixture
def product_post_data(create_file, mock_product_csv):
    return {
        "file": create_file(filename="product.csv", data=mock_product_csv),
        "name": "TESTING123"
    }
    
@fixture(scope="module")
def mock_product_id(mock_partner_account, mock_product_csv):
    savior_id = mock_partner_account["_id"]
    partner = Partner(savior_id=savior_id, user_id=savior_id)
    product_id = partner.create_product(
        product_data=mock_product_csv, product_name="PRODUCT_ROUTES_SETUP"
    )
    yield product_id
    
def test_products_post(assert_route, partner_auth, product_post_data):
    assert_route(
        "/products",
        "post",
        partner_auth,
        ObjectId,
        data=product_post_data
    )

MOCK_PROCESS = {
    "activity": "testing",
    "activity_value": 12,
    "activity_unit": "kg",
    "activity_unit_type": "weight",
    "process": "TESTING THIS HERE"
}

def test_create_process_post(
    assert_route, db, mock_product_id, partner_auth, mock_product_csv
):
    stage_name = next(iter(mock_product_csv.stage))
    assert_route(
        f"/products/{mock_product_id}/{stage_name}/processes",
        "post",
        partner_auth,
        ObjectId,
        json=MOCK_PROCESS,
    )
    assert db.products.find_one(MOCK_PROCESS)
    
@fixture
def process_id(db):
    return db.products.find_one(MOCK_PROCESS, {"_id": 1})["_id"]

def test_update_product_process_put_patch(assert_route, partner_auth, db, process_id):  
    UPDATES = {
        "put": "PUT UPDATE",
        "patch": "PATCH UPDATE"
    }
    for method, name_update in UPDATES.items():
        assert_route(
            f"/products/processes/{process_id}",
            method,
            partner_auth,
            bool,
            json={
                "name": name_update
            }
        )
        assert db.products.find_one({"process": name_update})["_id"] == process_id
        db.products.update_one({"_id": process_id}, {"$set": MOCK_PROCESS})
        # resetting for the next tests that use process_id fixture
        
def test_product_process_delete(assert_route, process_id, partner_auth):
    assert_route(
        f"/products/processes/{process_id}",
        "delete",
        partner_auth,
        bool,
    )

def test_product_get(mock_product_id, api):
    assert_route_successful(api.get(f"/products/{mock_product_id}"), dict)
    
@pytest.mark.parametrize(
    ("limit", "skip"),
    [(0, 0), (0, 2), (1, 1), (None, None)]
)
def test_search_products(limit, skip, api, db):
    def assert_response(res, should_have_more: bool = None):
        assert res.status_code == 200
        products = decode_response(res)["content"]
        print(products, list(db.emission_factors.find({"name": PRODUCT_NAME})))
        assert isinstance(products["has_more"], bool)
        if isinstance(should_have_more, bool):
            assert products["has_more"] == should_have_more
        return products
    if all(x == None for x in [limit, skip]):
        res = api.get("/products", query_string={"q": PRODUCT_NAME})
        assert_response(res, False)
    else:
        res = api.get("/products", query_string={"limit": limit, "skip": skip})
        products = assert_response(res)
        if all(x == 0 for x in [limit, skip]):
            assert len(products) > 0
        
def test_delete_product(
    assert_route, mock_product_id, partner_auth
):
    assert_route(
        f"/products/{mock_product_id}",
        "delete",
        partner_auth,
        bool,
    )


# def test_star_product_post(assert_route, mock_product_id, partner_auth):
#     assert_route(
#         f"products/{mock_product_id}/stars",
#         "post",
#         partner_auth,
#         bool 
#     )

# def test_star_product_delete(assert_route, mock_product_id, partner_auth):
#     assert_route(
#         f"products/{mock_product_id}/stars",
#         "delete",
#         partner_auth,
#         bool 
#     )