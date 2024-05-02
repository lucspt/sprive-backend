from pytest import fixture
from bson import ObjectId
import pytest
from tests.utils import decode_response
from root.partner import Partner

@fixture(scope="module")
def mock_product(mock_partner_account, mock_product_csv):
    savior_id = mock_partner_account["_id"]
    partner = Partner(savior_id=savior_id, user_id=savior_id)
    product_id = partner.create_product(
        product_data=mock_product_csv,
        product_name="testing partner routes"
    )
    return product_id

def test_products_get(partner_auth, assert_route):
    assert_route("/saviors/products", "get", auth=partner_auth, expect=list)

def test_product_names_get(assert_route, partner_auth):
    assert_route("/saviors/product-names", "get", auth=partner_auth, expect=list)
    
@fixture(scope="module")
def mock_product_endpoint(mock_product):
    return f"/saviors/products/{mock_product}"
    
def test_product_get(assert_route, partner_auth, mock_product_endpoint):
    assert_route(mock_product_endpoint, "get", auth=partner_auth, expect=(dict, None))

    
def test_products_patch(assert_route, partner_auth, mock_product_endpoint):
    assert_route(
        mock_product_endpoint,
        "patch",
        partner_auth,
        bool,
        json={"name": "random_rename"}
    )
    
def test_published_products_post(
    api,
    partner_auth, 
    assert_route, 
    mock_partner_account,
    mock_product_csv
):
    with pytest.raises(Exception):
        api.post(
            "/saviors/published-products", 
            headers=partner_auth,
            json={"product_id": ObjectId()}, #fail with non existant product
        )
    savior_id = mock_partner_account["_id"]
    partner = Partner(savior_id=savior_id, user_id=savior_id)
    product_id = partner.create_product(
        product_data=mock_product_csv, 
        product_name="testing-publish-endpoint"
    )
    assert_route(
        "/saviors/published-products", 
        "post", 
        partner_auth,
        bool,
        json={"product_id": str(product_id)}
    )
    
def test_published_products_delete(partner_auth, assert_route, mock_product):
    assert_route(
        f"saviors/published-products/{mock_product}", 
        "delete", 
        partner_auth,
        bool,
    )
    
def test_product_delete(assert_route, partner_auth, mock_product_endpoint, db, mock_product):
    content = assert_route(mock_product_endpoint, "delete", auth=partner_auth, expect=bool)
    assert content 
    assert not db.products.find_one({"product_id": mock_product})
    
def test_company_teams(partner_auth, assert_route):
#     assert_route("/saviors/company-teams", "post", partner_auth, )
    assert_route("/saviors/company-teams", "get", partner_auth, list)
    
def test_company_users_get(partner_auth, assert_route):
    assert_route("/saviors/company-users", "get", partner_auth, list)
    
def test_company_tree_get(partner_auth, assert_route):
    user_account = {
        "role": "user",
        "password": "rand",
        "username": "test",
        "email": "lovelife123@sprive.com",
        "team": "Saviors"
    }
    assert_route(
        "/saviors/company-tree", 
        "post", 
        partner_auth, 
        ObjectId, 
        json=user_account
    )
    company_tree = assert_route(
        "/saviors/company-tree", "get", partner_auth, list
    )
    print(company_tree)
    username=user_account["username"]
    assert list(filter(lambda x: x["username"] == username, company_tree))
    
# here we do some more in depth testing, actually asserting functionality,
# since these functions are not a part of the Partner class


NUM_PROCESSED_LOGS = 10
NUM_UNPROCESSED_LOGS = 5
NUM_FILES = 1
FILE_ID = ObjectId()
    

@fixture(autouse=True)
def partner_routes_startup(db, mock_partner_account):
    savior_id = mock_partner_account["_id"]
    logs = [
        {
            "savior_id": savior_id,
            "co2e": 1,
            "source_file": { "id": FILE_ID },
        } for _ in range(NUM_PROCESSED_LOGS)
    ]
    unprocessed_logs = [
        {
            "savior_id": savior_id,
            "source_file": { "id": FILE_ID },
        } for _ in range(NUM_UNPROCESSED_LOGS)
    ]
    logs += unprocessed_logs
    db.logs.insert_many(logs)
    yield
    db.logs.delete_many({"savior_id": mock_partner_account["_id"]})

@pytest.mark.parametrize(
    ("endpoint", "api_kwargs", "assertion"),
    [
        (
            "/saviors/files",
            {},
            lambda res: (
                len(res) == NUM_FILES
            ),
        ),
        (
            f"/saviors/files/{FILE_ID}",
            {"query_string": {"unprocessed-only": True}},
            lambda res: (
                len(res) == NUM_UNPROCESSED_LOGS
                and all(not x.get("co2e") for x in res)
            ),
        ),
    ]
)
def test_get_files(
    endpoint, 
    api_kwargs,
    assertion,
    api,
    partner_auth,
):
    res = api.get(endpoint, headers=partner_auth, **api_kwargs)
    res = decode_response(res)
    assert assertion(res["content"])

@pytest.mark.parametrize(
    (
        "missing_columns_err", 
        "form_data_err", 
        "form_data", 
        "file_data", 
    ),
    [
        (True, False, {}, {}),
        (False, True, {}, {}),
        (
            False, 
            False, 
            {"category": "123", "scope": "2"},
            [{
                "activity": "test", 
                "value": 10, 
                "unit_type": "weight", 
                "unit": "kg",
                "scope": "2",
                "category": "random"
            }]        
        )
    ]
)              
def test_post_files(
    missing_columns_err,
    form_data_err,
    form_data,
    file_data,
    partner_auth,
    api,
    create_file,
    db
):
    call = lambda **kwargs: api.post(
        "/saviors/files", headers=partner_auth, **kwargs
    )
    if missing_columns_err:
        res = call(data={
            "file[]": create_file("fail.csv", [{"this": "shouldfail"}]),
            "scope": "2",
            "ghg_category": "3.1",
            "category": "random",
            "unit_type": "weight"
        })
        assert res.status_code >= 400 
        assert "Missing data" in decode_response(res)["content"]
    elif form_data_err:
            res = call(data={"form_data": "err"})
            assert res.status_code >= 400
    else:
        res = call(data={
            **form_data,
            "file": create_file("success.csv", file_data)
        })
        assert res.status_code >= 200 < 300
        file_id = decode_response(res)["content"]
        inserted = list(db.logs.find({"source_file.id": ObjectId(file_id)}))
        assert len(inserted) == len(file_data)
        for log in inserted:
            assert log.keys() & form_data.keys()