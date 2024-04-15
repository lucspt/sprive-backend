"""Utils, common assertions and functions for testing."""

from flask import Response, Flask
from typing import Callable
import json 
from typing import Type, Any

def decode_response(response: Response) -> Any:
    """Get the contents of a flask Response
    
    Args:
        response (Response): The response to decode
    
    Returns:
        The content - Any
    """
    try:
        return json.loads(
            response.get_data().decode("utf-8")
        )
    except Exception as e:
        return None
    

def create_account_test(
    endpoint: str,
    duplicate_error: bool,
    duplicate_account: dict, 
    api: Flask.test_client, 
    username_to_delete: str = "LOVELIFE",
    extra_account_data: dict = {},
) -> dict:
    """Test an account creation endpoint
    
    Perform tests for mock duplication scenarios, i.e requesting 
    account creation with a taken email, as well as successful 
    account creations.
    
    Args:
        endpoint (str): The endpoint to post to.
        duplicate_error (bool): Whether or not to perform a duplication error.
        duplicate_account (dict): A mock account with unique-indexed fields 
            already taken
        api (Flask.test_client): The testing client
        username_to_delete: The username to delete after testing the endpoint.
        extra_account_data (dict): Extra account data to post as json when 
            `duplicate_error` is False
        
    Returns:
        The decoded api POST result
    """
    
    if duplicate_error:
        res: Response = api.post(endpoint, json=duplicate_account)
        assert res.status_code == 409
    else:
        account = {
            "company_name": "random",
            "email": "RANDOMTESTER@LOVELIFE.COM",
            "username": username_to_delete,
            "password": "testing",
            "region": "testing",
            **extra_account_data
        }
        res: Response = api.post(endpoint, json=account)
        print(res, "RES")
        assert res.status_code == 201
        res: dict = decode_response(res)["content"]
        assert res.keys() & account.keys()
        return res
    
def is_ok(response: Response) -> bool:
    """Check whether a response is `OK`
    
    Args:
        response (Response): The endpoint Response
    
    Returns:
        A boolean indicating if the response is OK
    """
    status_code = response.status_code
    return status_code >= 200 and status_code <= 299

def _get_serialized_instance(instance: Type) -> Type:
    """Json serialize an dtype
    
    Args:
        instance (Type): The type to serialize
        
    see `get_json_serialized_instance` for more
    
    Returns: the serialized instance
    """
    if callable(instance):
        return type(json.loads(json.dumps(instance(), default=str)))
    elif instance is None:
        return type(None)
    else:
        return instance

def get_json_serialized_instance(instances: Type | tuple[Type, ...]):
    """Serialize a or a tuple of dtypes
    
    Used for asserting that a flask view returns the correct dtype.
    As it might return an ObjectId, for example, we json serialize it,
    this function mocks that.
    
    Args:
        instances (Type) | tuple[Type, ...]: The types to serialize
    
    Returns: 
        The serialized dtypes
    """
    if isinstance(instances, tuple):
        return tuple(_get_serialized_instance(instance) for instance in instances)
    else:
        return _get_serialized_instance(instances)
    

def assert_route_successful(
    response: Response, expected_instance: Type | tuple[Type, ...]
) -> Any:
    """Assert a route.
    
    Check if a route will create an OK response,
    and return the dtype it is expected to
    
    Args:
        response (Response): The flask response to assert
        expected_instance (Type | tuple[Type, ....]): The expected
            instance or instances response should return
    
    Returns: 
        The decoded response content
    
    Raises:
        AssertionError: When the response is not OK,
            or the expected dtype does not match the response result
    """
    assert is_ok(response), "Response not OK"
    content = decode_response(response)["content"]
    instance_result = get_json_serialized_instance(instances=expected_instance)
    assert isinstance(
        content, instance_result
    ), f"Expected {expected_instance} dtype result, recieved {type(content)}"
    
    return content
    
def assert_route_unauthorized(response: Response) -> None:
    """Assert a route returns 401 (UNAUTHORIZED)
    
    Args:
        response (Response): The flask response to assert
    
    raises:
        AssertionError: When the route does not return a stust_code of 401
    """
    assert response.status_code == 401   
 
def assert_route(
    api_call: Callable,
    endpoint: str,
    auth_headers: dict, 
    expected_instance: Type,
    **request
) -> Any:
    """Assert a route is functional
    
    Any keyword arguments passed will be sent as a request.
    Authorization headers are combined with request headers
    automatically.
    
    Args:
        api_call (Callable): A function that invokes an Flask.test_client
            method. One of get, post, put patch, delete
        endpoint (str): The endpoint to make a request to.
        auth_headers (dict): The authorization headers to pass to 
            test the route is successful
        expected_instance (Type): The expected instance the route 
            should return
        
    Returns:
        The decoded response of the route that was successful.
        
    Raises:
        AssertionError: When either the `api_cal` with authorization is unsuccessful
            or the `api_call` without authorization was successful
    """
    # when we mock files, they close after an endpoint is called, 
    # we need to make a copy of the request to avoid those errors
    # as we will pass the same request twice
    request_copy = json.loads(json.dumps(request, default=str))
    content = assert_route_successful(
        api_call(
            endpoint,
            headers={**auth_headers, **request.pop("headers", {})}, 
            **request
        ), 
        expected_instance
    )
    assert_route_unauthorized(api_call(endpoint, **request_copy))
    return content
    