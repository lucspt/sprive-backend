from api.main.router import bp 
from pymongo import MongoClient
from bson import ObjectId
from flask import request
from api.helpers import savior_route, route 
from root.saviors.partner import Partner
from root.saviors.user import User
    
@bp.route("/pledges/<string:id>/stars", methods=["POST", "DELETE"])
@savior_route 
def star_pledge(savior: Partner | User, id: str) -> bool:
    """Star and unstar a pledge"""
    return savior.handle_stars(
            id=ObjectId(id),
            resource="pledges",
            delete=request.method == "DELETE"
        )

@bp.route("/products/<string:id>/stars", methods=["POST", "DELETE"])
@savior_route
def star_product(savior: Partner | User, id: str) -> bool:
    """Star and unstar a product"""
    return savior.handle_stars(
        id=id, 
        resource="products",
        delete=request.method == "DELETE"
    )


@bp.get("/pledges")
@route(needs_db=True)
def get_pledges(client: MongoClient) -> list:
    """Get pledges from db"""
    return list(client.spt.pledges.find({}))