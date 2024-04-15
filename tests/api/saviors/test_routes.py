from tests.utils import decode_response
import pytest
from typing import Callable

"""Since we are testing routes that 
are common between partners and users
we must keep them separate.
"""
    
parametrize_update_profile_route = (
    ("update", "should_raise"),
    [
        ({ "name": "random"}, False),
        ({"random": "fails"}, True)
    ]
)  
def assert_update_profile(
    update: dict, 
    should_raise: bool, 
    auth: dict, 
    assert_route: Callable
) -> None:
    def _assert() :
        assert_route(
        "/saviors",
        "put",
        auth,
        bool,
        json=update
    )
    if should_raise:
        with pytest.raises(Exception):
            _assert()
    else:
        _assert()

@pytest.mark.parametrize(*parametrize_update_profile_route) 
def test_update_partner_profile(
    update, should_raise, assert_route, partner_auth
):
    assert_update_profile(
        update, should_raise, partner_auth, assert_route
    )
    
ASSERT_SAVIOR_KEYS = {
    "savior_id", 
    "email",
    "username",
    "joined",
}
    
@pytest.mark.parametrize(*parametrize_update_profile_route) 
def test_update_user_profile(
    update, should_raise, assert_route, user_auth
):
    assert_update_profile(
        update, should_raise, user_auth, assert_route
    )
    
def test_get_current_partner(assert_route, api, partner_auth):
    content = assert_route(
        "/saviors",
        "get",
        partner_auth,
        dict,
    )
    assert content.keys() & ASSERT_SAVIOR_KEYS.union({
        "company_email",
        "company",
        "company_id",
        "bio",
        "user_id",
        "team",
        "measurement_categories",
        "region",
    })

def test_get_current_user(
    assert_route, user_auth
):
    content = assert_route(
        "/saviors",
        "get",
        user_auth,
        dict
    )
    assert content.keys() & ASSERT_SAVIOR_KEYS.union({
        "current_pledge", "spriving"
    })
    
def test_user_logs_get(user_auth, assert_route):
    assert_route(
        "/saviors/logs",
        "get",
        user_auth,
        dict,
    )
    
def test_partner_logs_get(assert_route, partner_auth):
    assert_route(
        "/saviors/logs",
        "get",
        partner_auth,
        list,
    )
    
get_data_route_params = lambda logs_collection: (
    ("collection", "query_type", "filters", "expected_return_instance", "raises"),
    [
        (logs_collection, "find", {}, list, False),
        (
            logs_collection, 
            "aggregate", 
            [{"$project": {"co2e": 1, "savior_id": 1}}], 
            list,
            False
        ),
        (logs_collection, "aggregate", {}, list, True)
    ]
    
)

@pytest.mark.parametrize(*get_data_route_params("logs"))
def test_partners_data_get(
    collection, 
    query_type,
    filters,
    expected_return_instance,
    assert_route,
    raises,
    mock_partner_account,
    partner_auth
):
    test = lambda: (
        assert_route(
            "/saviors/data",
            "post",
            partner_auth,
            expected_return_instance,
            json={
                "collection": collection,
                "query_type": query_type,
                "filters": filters
            }
        )
    )
    
    if raises:
        with pytest.raises(Exception):
            test()
    else:
        res = test()
        savior_id = str(mock_partner_account["_id"])
        for log in res:
            assert savior_id == log["savior_id"]

@pytest.mark.parametrize(*get_data_route_params("product_logs"))
def test_users_data_get(
    collection, 
    query_type,
    filters,
    expected_return_instance,
    assert_route,
    raises,
    mock_user_account,
    user_auth
):
    test = lambda: (
        assert_route(
            "/saviors/data",
            "post",
            user_auth,
            expected_return_instance,
            json={
                "collection": collection,
                "query_type": query_type,
                "filters": filters
            }
        )
    )
    
    if raises:
        with pytest.raises(Exception):
            test()
    else:
        res = test()
        savior_id = str(mock_user_account["_id"])
        for log in res:
            assert savior_id == log["savior_id"]