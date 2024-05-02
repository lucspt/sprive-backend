"""Exceptions for CRUD operation failures.

Mainly to help infer relevant response status codes 
to return from the api when certain operations fail.
We catch them in the route decorators found in api/helpers.py.
"""

from typing import override

class ExceptionWithStatusCode(Exception):
    """Exception with a api response status code
    
    Attribtues:
        status_code: This code will be returned from the api response.
            Defaults to 400.
    """
    @override
    def __init__(self, *args: object, status_code: int = 400) -> None:
        self.status_code = status_code
        super().__init__(*args)
        
class UnauthorizedError(ExceptionWithStatusCode):
    """Unauthorized error exception
    
    Raise when a request is unauthorized
    
    Attributes:
        status_code 401.
    
    see parent class for more
    """
    
    def __init__(self, *args: object) -> None:
        super().__init__(*args, status_code=401)

class ResourceNotFoundError(ExceptionWithStatusCode):
    """Error for resources not found.
    
    This error is intended to be raised when 
    a requested collection resource is not found.
    
    For example, a request to mark a task with 
    an id of 123 as complete, but no such task with the id exists.
    
    Attributes:
        status_code: 404
    
    see parent class for more
    """
    def __init__(self, *args: object) -> None:
        super().__init__(*args, status_code=404)
    
    

class InvalidMediaTypeError(ExceptionWithStatusCode):
    """Error for invalid media types
    
    Raise this error when an invalid file is uploaded.
    
    Attributes:
        status_code: 415
    
    see parent class for more
    """
    def __init__(self, *args: object) -> None:
        super().__init__(*args, status_code=415)
        
class MissingRequestDataError(ExceptionWithStatusCode):
    """Error to raise when a request is missing data.
    
    Attributes:
        status_code: 400
    """
    def __init__(self, *args: object) -> None:
        super().__init__(*args, status_code=400)
        
class InvalidRequestDataError(ExceptionWithStatusCode):
    """Error for invalid requests.
    
    Raise this when a request contains invalid data,
    
    For example, a PATCH request tries to modify
    a field like `joined` which is the date of their account
    creation. 
    
    Attributes:
        status_code: 400
    """
    
    def __init__(self, *args: object) -> None:
        super().__init__(*args, status_code=400)
        
class ResourceConflictError(ExceptionWithStatusCode):
    """Error for conflicting resources
    
    Raise this when a request tries to create 
    a resource with data that must be unique, 
    and is already in use.
    
    Example:
        When a partner tries to sign up for an account
        with an email that is already in use
    
    Attributes:
        status_code: 409
    """
    def __init__(self, *args: object) -> None:
        super().__init__(*args, status_code=409)

