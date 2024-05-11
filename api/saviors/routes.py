"""/saviors routes

Endpoints for the current savior, i.e the one making the request.
Provides data only accessible by the savior themselves. All routes
are wrapped with `savior_route` and require authentification

It's likely best to refrain from this route when an endpoint
uri becomes more than 3 levels deep. 

For example:
    /saviors/tasks/<task_id>/assignees 
    
    In this case, the /saviors/tasks routes
    can be translated to /tasks like so:
    
    /tasks/<task_id>/assignees
"""

from flask import request
from api.saviors.router import bp
from api.helpers import savior_route
from root.partner import Partner
from root.user import User

@bp.put("/", strict_slashes=False)
@savior_route
def update_profile(savior: User | Partner) -> bool:
    """PUT method to /saviors
    
    Update mutable fields of a savior account
    
    Expected json:
        username (str): The username update
        password (str): The password update
        name (str): The name update
        email (str) The email update
    
    Any other requested update will return an error Response
    """
    return savior.update_profile(request.json)

@bp.get("/", strict_slashes=False)
@savior_route
def get_savior(savior: User | Partner) -> dict:
    """GET method for /saviors endpoint
    
    Returns:
        The requesting savior's account
    """
    return savior.savior

@bp.get("/logs")
@savior_route
def logs(savior: User | Partner) -> dict | list:
    """GET method for /saviors/logs endpoint
    
    Returns:
        A list of dictionaries containing logs if request is from partner
        otherwise a dictionary with fields: logs, has_more
    """
    args = request.args.get
    print("HERERE")
    return savior.logs(
        limit=int(args("limit", 0)),
        skip=int(args("skip", 0))
    )
    
@bp.route("/data", methods=["POST"])
@savior_route 
def handle_data(savior: Partner | User) -> list | dict:
    """POST method for /saviors/data
    
    Access to mongodb find and aggregation methods on db collections.
    
    Only data of the requesting savior can be retrieved
    
    Expected json:
        collection (str): a valid collection
        query_type (Literal[aggregate, find]): Whether to call find() or aggregate on the collection
        filters ([dict | list[dict]]): A dict filters to find() or a aggregate pipeline
        
    Returns:
         a list or dict with the resulting data
    """
    return savior.get_data(**request.json)