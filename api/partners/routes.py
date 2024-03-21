from api.helpers import (
    route, 
    create_account,
    login, 
    check_email_availability,
)
from api.partners.router import bp 
from pymongo import MongoClient, database
from flask import request
from bson import ObjectId


@bp.post("/", strict_slashes=False)
@route(needs_db=True, send_return=False)
def create_partner(client: MongoClient):
    partner = request.json 
    return create_account(
        client=client, 
        savior_type="partners",
        username=partner["username"],
        password=partner["password"],
        email=partner["email"],
        include_token=False,
        region=partner["region"]
    )
    


@bp.post("/login")
@route(needs_db=True, send_return=False)
def partner_login(client: MongoClient):
    """Login a user"""
    partner = request.json
    return login(
        collection=client.spt.partners, 
        savior_type="partners",
        include_token=request.args.get("token") == "include",
        password=partner["password"],
        username=partner["username"],
    )


@bp.get("/emails/<string:email>")
@route(needs_db=True)
def uniquify_emails(client: MongoClient, email: str) -> dict:
    """This endpoint will recieve an email 
    from a two step form that handles account creation.
    It checks if the email is already in use"""
    return check_email_availability(collection=client.spt.partners, email=email)
    

    

def get_partner_card(db: database.Database, id: str | None = None) -> dict:
    """this provides the data for a partner card 
    that the public has access to"""
    partner_card = db.partners.aggregate(
        [
            {"$match": {"_id": id}},
            {"$lookup": {
                    "from": "pledges",
                    "foreignField": "savior_id",
                    "localField": "_id",
                    "as": "pledges"
                }
            },
            {"$unwind": "$pledges"},
            {
                "$lookup": {
                    "from": "stars",
                    "foreignField": "resource_id",
                    "localField": "pledges._id",
                    "as": "pledges.stars",
                }
            },
            {"$group": {
                    "_id": None,
                    "name": {"$first": "$username",},
                    "pledges": {"$push": {   
                        "name": "$pledges.name",
                        "_id": "$pledges._id",
                        "description": "$pledges.description",
                        "image": "$pledges.image",
                        "started": "$pledges.created",
                        "co2e": "$pledges.co2e",
                        "status": "$pledges.status",
                        "recurring": "$pledges.recurring",
                        "frequency": "$pledges.frequency",
                        "frequency_value": "$pledges.frequency_value",
                        "stars": "$pledges.stars.savior_id"
                        },
                    },
                    "emissions_saved": {"$sum": "$pledges.co2e"},
                }
            },
        ]
    ).next()
    partner_card["products"] = list(
        db.products.aggregate(
            [
                {"$match": {"savior_id": id, "published": True}},
                {
                    "$group": {
                        "_id": "$product_id", 
                        "co2e": {"$sum": "$co2e"},
                        "keywords": {"$first": "$keywords"},
                        "category": {"$first": "$category"},
                        "product_id": {"$first": "$product_id"},
                        "rating": {"$first": "$rating"},
                        "name": {"$first": "$name"},
                        "unit_types": {"$first": "$unit_types"},
                        "published_at": {"$first": "$published_at"}
                    }
                },
                {"$sort": {"created": -1, "co2e": -1}}
            ]
        )
    )
    partner_card["_id"] = id
    return partner_card

@bp.route("/", strict_slashes=False, methods=["GET"])
@bp.route("/<string:id>", strict_slashes=False, methods=["GET"])
@route(needs_db=True)
def get_saviors(client: MongoClient, id: str | None = None) -> list | dict:
    """Get a savior or all saviors from database"""
    db = client.spt
    if id:
        return get_partner_card(db, id=ObjectId(id))
    else:
        return list(
            db.partners.aggregate(
                [
                    {
                        "$lookup": {
                            "from": "pledges",
                            "foreignField": "savior_id",
                            "localField": "_id",
                            "as": "emissions_saved",
                            "pipeline": [
                                {
                                    "$group": {
                                        "_id": "$savior_id",
                                        "co2e": {"$sum": "$co2e"}
                                    }
                                },
                                { "$project": {"_id": 0} }
                            ]
                        }
                    },

                    {
                        "$project": {
                            "pledges_made": {"$size": "$emissions_saved"},
                            "username": 1,
                            "emissions_saved": {"$arrayElemAt": ["$emissions_saved.co2e", 0]},
                            "logo": 1,
                            "joined": 1
                        }
                    }
                ]
            )
        )