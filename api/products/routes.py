from api.products.router import bp 
from flask import request
from api.helpers import savior_route, file_to_df, route
from bson import ObjectId
from root.saviors.partner import Partner
from typing import Literal
from pymongo import MongoClient

@bp.route("/<string:product_id>/<string:stage_name>/processes", methods=["POST"])
@savior_route(error_message="Invalid stage name")
def create_process(
    savior: Partner,
    product_id: str, 
    stage_name: Literal["sourcing", "assembly", "processing", "transport"]
) -> ObjectId:
    """Create a new process for a product given the product id, and stage name 
    the requested process should be created under"""
    if stage_name not in ["sourcing", "assembly", "processing", "transport"]:
        raise ValueError
    return savior.create_product_process(
        process_data=request.json, product_id=product_id, stage=stage_name
    )
    
@bp.route("processes/<string:process_id>", methods=["PUT", "DELETE"])
@savior_route
def handle_product_processes(savior: Partner, process_id: str):
    method = request.method 
    if method == "PUT":
        return savior.update_product_process(
            process_id=process_id, process_update=request.json
        )
    elif method == "DELETE":
        return savior.delete_product_process(process_id=process_id)

    
@bp.post("/", strict_slashes=False)
@savior_route(error_code=409)
def create_product(savior: Partner):
    """Create a product for the savior! 
    This endpoint accepts a csv / excel sheet and a product name and does just that
    """
    return savior.create_product(
        product_data=file_to_df(file=request.files.get("file")),
        product_name=request.form.get("name")
    )
    
@bp.get("/<string:id>")
@route(needs_db=True)
def get_product(client: MongoClient, id: str) -> dict:
    """Get a published product, 
    we aggregate on products so that we can have access to the product stages
    """
    print("heyy")
    product = client.spt.products.aggregate(
        [
            {"$match": {"product_id": ObjectId(id)}},
            {
                "$group": {
                    "_id": "$stage",
                    "keywords": {"$first": "$keywords"},
                    "co2e": {"$sum": "$co2e"},
                    "product_id": {"$first": "$product_id"},
                    "published": {"$first": "$published"},
                    "last_updated": {"$max": "$last_updated"},
                    "name": {"$first": "$name"},
                    "unit_types": {"$first": "$unit_types"},
                    "stars": {"$first": "$stars"},
                    "name": {"$first": "$name"},
                    "processes": {"$push": {
                    "_id": "$_id",
                    "activity": "$activity",
                    "process": "$process",
                    "activity_unit": "$activity_unit",
                    "activity_value": "$activity_value",
                    "co2e": "$co2e"
                    }}
                }
            },
            {"$group": {
                "_id": None, 
                "stages": {
                    "$push": {
                        "co2e": {"$sum": "$processes.co2e"},
                        "num_processes": {"$size": "$processes"},
                        "stage": "$_id",
                        "processes": "$processes",
                        "last_updated": "$last_updated",
                    }
                },
                "co2e": {"$sum": "$co2e"},
                "unit_types": {"$first": "$unit_types"},
                "name": {"$first": "$name"},
                "stars": {"$first": "$stars"},
                "last_updated": {"$first": "$last_updated"},
                "keywords": {"$first": "$keywords"},
                "product_id": {"$first": "$product_id"},
            }
            },
        ]
    )
    response = product.next() if product.alive else {}
    return response
   
@bp.get("/", strict_slashes=False)
@route(needs_db=True)
def get_products(client: MongoClient) -> list:
    """This endpoint gives access to published products, 
    we aggregate through the emission factors collection
    as there is no need for product stages
    """
    products = client.spt.emission_factors
    match = {"$match": {"product_id": {"$exists": True}}}
    project =  {
            "$project": {
                "name": 1,
                "co2e": 1,
                "unit_types": 1,
                "keywords": 1,
                "last_updated": 1,
                "product_id": 1,
                "rating": 1,
            }
        }
    search_queries = request.args.to_dict()
    sort = {"last_updated": -1}
    if "activity" in search_queries:
        search_queries["$text"] = {"$search": search_queries.pop("activity")}
        project["$project"].update({"relevance": {"$meta": "textScore"}})
        sort["relevance"] = -1
    limit = int(search_queries.pop("limit", 0))
    skip = int(search_queries.pop("skip", 0))
    match["$match"].update(search_queries)
    pipeline = [match, project, {"$sort": sort}]
    if limit:
        pipeline.append({"$limit": limit + 1})
    if skip:
        pipeline.append({"$skip": skip})
    res = list(products.aggregate(pipeline))  
    if limit:
        res, has_more = res[:limit], bool(res[limit:])
    else:
        has_more = False
    return {"products": res, "has_more": has_more}

@bp.delete("/<string:id>")
@savior_route
def delete_product(savior: Partner, id: str) -> bool:
    """Handle deletion of products"""
    return savior.delete_product(product_id=id)

@bp.patch("/<string:id>")
@savior_route(error_code=409)
def update_product(savior: Partner, id: str) -> bool:
    """Update a product, the only editable field is the product name.
    The rest is handle by stage / process specific endpoints.
    """
    return savior.update_product(name_update=request.json["name"], product_id=id)
    
    