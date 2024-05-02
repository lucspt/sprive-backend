import pytest
import api.helpers as helpers
from pymongo import MongoClient
from flask import Response
import pandas as pd
from werkzeug.datastructures import FileStorage
from tests.utils import decode_response
from exceptions import ResourceNotFoundError
from root.user import User
from root.partner import Partner

def route_wrapper_test(
    route_response,
    send_return,
    expected_error_code, 
    error_check, 
    success_code, 
    error_message,
    wrapped_args,
    mock_route,
    flask_app,
    route_wrapper
):
    with flask_app.app_context():
        res = route_wrapper(
            send_return=send_return, 
            success_code=success_code, 
        )(mock_route)(*wrapped_args)
        if send_return:
            assert isinstance(res, Response)
            decoded_res = decode_response(res)["content"]
            if error_check:
                assert res.status_code == expected_error_code
                assert decoded_res == error_message
            else:
                assert res.status_code == success_code
                assert decoded_res == route_response
  
# Both route wrappers are essentialy the same in what we expect them to return,
# But there are a couple differences, for example in authorization and what 
# arguments they pass to the functions they wrap. Because of this, they do 
# not have a common function that they call for us to test. See helpers.py for more.
# While it does cause some repitition in testing them, it's less required control 
# flow for each route / request, meaning faster responses. It also is more readable.
# So currently, wee keep it that way, and here we just define common parameters 
# to test for both,

route_wrapper_params = (
      ( 
        "send_return", 
        "expected_error_code",
        "error_check", 
        "success_code", 
        "error_message", 
        "wrapped_args",
        "route_response",
     ),
    [
        (True, 404, ResourceNotFoundError, 200, "Error", ("arg1",), None),
        (True, None,  None, 209, None, ("arg2",), "SUCCESS")
    ]  
)
              
@pytest.mark.parametrize(
    ("needs_db", *route_wrapper_params[0]),
    [
      (_bool, *argvals) for _bool, argvals  in zip((True, False), route_wrapper_params[-1])
        # add a True and False mock scenario in the two tests.
    ]  
)
def test_route(
        needs_db,
        send_return,
        expected_error_code, 
        error_check, 
        success_code, 
        error_message,
        wrapped_args,
        route_response,
        flask_app,
):
    with flask_app.app_context():
        def mock_route(*args, **kwargs):
            if needs_db:
                assert isinstance(kwargs["client"], MongoClient)
            assert args == wrapped_args
            if error_check:
                raise error_check(error_message)
            return route_response
        route_wrapper_test(
            route_response=route_response,
            send_return=send_return,
            expected_error_code=expected_error_code,
            error_check=error_check,
            success_code=success_code,
            wrapped_args=wrapped_args,
            route_wrapper=lambda **kwargs: (
                helpers.route(needs_db=needs_db, **kwargs)
            ),
            mock_route=mock_route,
            flask_app=flask_app,
            error_message=error_message
        )

def test_savior_route_auth_requirements(api, partner_auth, user_auth):
    """Since we are verifying jwt in request 
    we need to actually make an api call to test this.
    
    For that see api/pytesting.
    
    It implements the tests for `savior_route`.
    
    """
    def _call(headers, savior_type="partners"):
        return api.get(
            "/pytesting/mock-jwt-required", 
            headers=headers,
            query_string={
                "savior_type": savior_type
            }
        )
    res = _call(partner_auth)
    assert res.status_code == 200
    res = _call({})
    assert res.status_code == 401
    api.delete("/saviors/logout")
    res = _call(user_auth, "users")
    assert res.status_code == 200
    res = _call({}, "users")
    assert res.status_code == 401
        
@pytest.mark.parametrize(
    ("filename", "to_file_fn", "should_raise"),
    [
        ("mock.csv", "to_csv", False),
        ("invalid.json", "to_json", True),
        ("mock.xlsx", "to_excel", False),
        ("mock.xls", "to_excel", False)
    ]
)
def test_file_to_df(filename, to_file_fn, should_raise, tmp_path):
    filepath = tmp_path / filename
    df = pd.DataFrame.from_records([{"love": "life"}])
    to_file_fn = getattr(df, to_file_fn)
    to_file_fn(filepath, index=False)
    with open(filepath, "rb") as stream:
        file = FileStorage(stream=stream, filename=filename)
        if should_raise:
            with pytest.raises(Exception):
                helpers.file_to_df(file=file)
        else:
            res = helpers.file_to_df(file=file)
            assert isinstance(res, pd.DataFrame)
            pd.testing.assert_frame_equal(df, res) 
    

    
    
    