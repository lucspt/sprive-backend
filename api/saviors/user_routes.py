"""/saviors routes

Endpoints only relevant to a savior of type 'users'.
See ./routes.py for more.
"""

from api.saviors.router import bp 
from api.helpers import savior_route
from root.user import User
from flask import request

@bp.post("/logs")
@savior_route(success_code=201)
def log_product_emissions(savior: User) -> dict:
    """POST method for /saviors/logs
    
    This view is only for users and handles their requested product logs
    
    currently partner's post logs to /saviors/files
    
    Expected json:
        product_id (str): The id of the product to calculate emissions for
        value (int): The number to multiply the product's co2e by when logging
    
    Returns:
        a dictionary of the inserted log
    """
    json = request.json 
    return savior.log_product_emissions(
        product_id=json["product_id"], 
        value=json["value"],
    )

@bp.get("/stars")
@savior_route
def get_starred_products(savior: User) -> dict:
    """GET endpoint for /saviors/stars.
    
    Get a user's starred products.
    
    Query params:
        limit (int): Optional. limit the length of the results. 
            Defaults to 0.
        skip (int): Optional. The amount of documents to skip
            for pagination. Defaults to 0
    
    Returns: 
        A list of the user's starred products.
    """
    get_arg = request.args.get
    return savior.starred_products(
        limit=int(get_arg("limit", 0)), skip=int(get_arg("skip", 0))
    )
    
@bp.get("/times-logged")
@savior_route
def times_logged(savior: User) -> int:
    """GET method for  /saviors/times-logged
    
    Query params:
        since_date (ISO string): The date to query from when counting logs
    
    Returns: 
        an int of how many logs the user has made since `since_date`
    """
    return savior.get_times_logged(
        since_date=request.args["since_date"]
    )
    
@bp.post("/sprivers")
@savior_route
def start_spriving(savior: User) -> bool:
    """POST method for /saviors/sprivers endpoint 
    
    Turn a savior into spriver, aka subscribe to membership
    
    Returns: 
        A boolean denoting whether the initiation was successful
    """
    return savior.start_spriving()

@bp.delete("/sprivers")
@savior_route
def stop_spriving(savior: User) -> bool:
    """DELETE method for /saviors/sprivers endpoint
    
    Cancel a spriver's subscription
    
    Returns: 
        A boolean denoting whether cancellation was successful
    """
    return savior.stop_spriving()

@bp.delete("/stars/<string:product_id>")
@savior_route
def delete_star(savior: User, product_id: str) -> bool:
    """DELETE method of /stars/<product_id> endpoint
    
    Unstar a product
    
    Path args:
        product_id (str): The product_id of the product to star
    
    Returns:
        A boolean denoting if the deletion took place
    """
    return savior.handle_stars(product_id=product_id, delete=True)

@bp.post("/stars/<string:product_id>")
@savior_route
def star_product(savior: User, product_id: str) -> bool:
    """POST method of /stars/<product_id> endpoint
    
    Star a product
    
    Path args:
        product_id (str): The id of the product to star
    
    Returns:
        A boolean indicating if the starring was successful
    """
    return savior.handle_stars(product_id=product_id, delete=False)

@bp.route("/current-pledge", methods=["POST", "PUT"])
@savior_route
def post_put_current_pledge(savior: User) -> bool:
    """POST and PUT for /saviors/pledge endpoint

    Create or update a user's current pledge
    
    Expects json containing a dictionary with fields:
        co2e (int): The amount of co2e the user is pledging
        frequency (Literal[day, week, year, month]): The frequency of the pledge
        message (str): Optional. A (maybe motivating) message to document the
            pledge with

    Returns:
        A boolean denoting if the operation was successful
    """
    return savior.pledge(pledge_document=request.json)

@bp.delete("/current-pledge")
@savior_route
def delete_current_pledge(savior: User) -> bool:
    """DELETE method for /saviors/pledge endpoint
    
    Set the current pledge of a user to None
    
    Returns:
        A boolean indicating if the deletion was succesful
    """
    return savior.undo_pledge()