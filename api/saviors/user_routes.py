from api.saviors.router import bp 
from api.helpers import savior_route
from root.saviors.user import User
from flask import request
from pymongo import errors as MongoErrors
from bson import ObjectId

@bp.get("/history")
@savior_route
def history(savior: User):
    """The history of a user
    
    This returns the products the user has requested calculations for
    w.r.t the `limit` query param"""
    return savior.history(limit=int(request.args.get("limit", 0)))


@bp.post("/logs")
@savior_route
def log_product_emissions(savior: User) -> ObjectId:
    """Log emissions for a user given a product.
    
    This function multiplies a product's co2e amount   
    by the POSTed value for a user"""
    json = request.json 
    return savior.log_product_emissions(
        product_id=json["product_id"], 
        value=json["value"],
    )
    
@bp.get("/logs")
@savior_route
def product_logs(savior: User) -> dict[str, list | bool]:
    args = request.args
    limit = int(args.get("limit", 0))
    return savior.logs(
        limit=limit, skip=int(args.get("skip", 0))
    )

@bp.get("/stars")
@savior_route
def get_starred_products(savior: User) -> list:
    get_arg = request.args.get
    return savior.starred_products(
        limit=int(get_arg("limit", 0)), skip=int(get_arg("skip", 0))
    )
    
@bp.route("/pledges", methods=["POST", "PUT"])
@savior_route
def make_pledge(savior: User) -> bool:
    return savior.make_pledge(request.json)

@bp.get("/logs/amount")
@savior_route(error_check=KeyError, error_message="Invalid `since date`")
def times_logged(savior: User) -> int:
    """Get how many times a user has logged since a given date"""
    print(request.args, "ssssss")
    return savior.get_times_logged(
        since_date=request.args["since_date"]
    )
    
@bp.post("/sprivers")
@savior_route
def start_spriving(savior: User) -> bool:
    """This route will handle when a `savior` want to start saving, AKA subscribe to membership.
    
    We call them saviors even if they aren't subscribed
    because they are already putting in the effort and that is already amazing.
    
    If only everything was free right!!!
    """
    return savior.start_spriving()

@bp.delete("/sprivers")
@savior_route
def stop_spriving(savior: User):
    """We set `spriving` to false, 
    to know why we named it `kinda stop saving`
    refer to `start_saving` route
    """    
    
    return savior.stop_spriving()