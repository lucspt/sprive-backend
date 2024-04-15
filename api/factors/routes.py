from flask import request, Response
from api.factors.router import bp 
from root.partner import Partner
from api.helpers import route, send
from pymongo import MongoClient
from api.helpers import savior_route

@bp.route("/", methods=["GET"], strict_slashes=False)
@savior_route(send_return=False)
def factors(savior: Partner) -> Response:
    """Emission factors search functionality"""
    # HAVE TO FIX THIS WHNE SETTING UP FACTORS SEARCH
    return "hey"
    # search_queries = request.args.to_dict()
    # projection = {"embeddings": 0}
    # sort = {}
    # if "activity" in search_queries:
    #     search_queries["$text"] = {"$search": f'\"{search_queries.pop("activity")}\"'}
    #     sort = {"$sort": {"relevance": -1}}
    #     projection["relevance"] = {"$meta": "textScore"}
    # limit = int(search_queries.pop("limit", 30))
    # skip = int(search_queries.pop("skip", 0))
    # if search_queries.pop("saved", False):
    #     search_queries["saved_by"] = savior.savior_id
    # def _get_pipeline():
    #     pipeline = [
    #         {
    #             "$match": {
    #                 "$or": [
    #                     {"savior_id": savior.savior_id}, 
    #                     {"source": {"$ne": "partners"}}
    #                 ],
    #                 **search_queries
    #             }
    #         }, 
    #         {
    #             "$project": projection
    #         },
    #         {"$skip": skip},
    #         {"$limit": limit}
    #     ]
    #     if sort:
    #         pipeline.append(sort)
    #     return pipeline
    # emission_factors = savior.db.emission_factors
    # result = emission_factors.aggregate(_get_pipeline())
    # max_results = emission_factors.count_documents(search_queries)
    # return send(
    #     status=200, 
    #     content=list(result), 
    #     max_results=max_results
    # )
    
    
@bp.post("/", strict_slashes=False)
@savior_route
def create_emission_factor(savior: Partner) -> bool:
    """Create an emission factor"""
    method = request.method 
    if method == "POST":
        return savior.create_factor(
            factor_document=request.json 
        )
        
        
@bp.delete("/factors/<string:factor_id>")
@savior_route
def delete_factor(savior: Partner, factor_id: str) -> int:
    """Delete a factor created by a savior"""
    return savior.db.emission_factors.delete_one(
        {"savior_id": savior.savior_id, "resource_id": factor_id}
    ).deleted_count

        
@bp.route("/calculations", methods=["POST"])
@savior_route
def calculate_emissions(savior: Partner) -> dict | list[dict]:
    """This is the endpoint for calculating emissions with a 
    plain emission factor
    """
    json = request.json 
    is_batched = request.json.get("batched", False)
    if is_batched:
        results = savior.ghg_calculator.calculate_batches(json.get("data"))
    else:
        results = savior.ghg_calculator(**json)
    return results

@bp.route("/<string:resource>/possibilities", methods=["GET"])
@route(needs_db=True)
def get_possibilities(client: MongoClient, resource: str) -> list:
    """This returns possibilities values for emission factor fields,
    e.g distic `region`, or `unit_type` values
    """
    return client.spt.emission_factors.distinct(resource)

@bp.get("/possibilities")
@route(needs_db=True)
def get_all_possibilities(client: MongoClient):
    emission_factors = client.spt.emission_factors
    possibilities = {}
    distinct = emission_factors.distinct
    for query_field in ["activity", "unit_types", "region", "source"]:
        possibilities[query_field] = distinct(query_field)
    return possibilities
