from typing import Callable, Type
from flask_jwt_extended import (
    create_access_token, 
    get_jwt, get_jwt_identity, 
    verify_jwt_in_request,
    set_access_cookies
)
from api.helpers import send
from root.saviors.partner import Partner
from root.saviors.user import User 
from functools import wraps
from datetime import datetime, timedelta, timezone

def savior_route(
    _func=None,
    *,
    send_return: bool = True,
    error_check: Type[Exception] = Exception,
    sucess_code: int = 200, 
    error_code: int = 400,
) -> Callable:
    """this wraps a route, verifies the jwt header, 
    and passes a `Savior` class to it, 
    
    it also will refresh an access token if neeeded
    """
    @wraps(_func)
    def _wrapper(func: Callable | None = _func) -> Callable:
        @wraps(func)
        def _inner(*args, **kwargs):
            try: 
                verify_jwt_in_request()
                savior_id = get_jwt_identity()
                jwt = get_jwt()
                savior_type, expiration = jwt["savior_type"], jwt["exp"]
                print(savior_id)
            except Exception as e:
                return send(content=e, status=401)
            try:
                if savior_type == "partners":
                    savior = Partner(savior_id=savior_id)
                elif savior_type == "users":
                    savior = User(savior_id=savior_id)
                res = send(
                    content=func(savior, *args, **kwargs), status=sucess_code
                ) if send_return else func(savior, *args, **kwargs)
                savior._close()
            except error_check as e:
                return send(content=e, status=error_code)
            should_refresh = datetime.timestamp(
                datetime.now(tz=timezone.utc) + timedelta(minutes=30)
            )
            if should_refresh > expiration:
                refreshed_token = create_access_token(
                    identity=savior_id,
                    additional_claims={"savior_type": savior_type}
                )
                set_access_cookies(res, refreshed_token)
            return res
        return _inner
    return _wrapper(_func) if _func else _wrapper



    
        