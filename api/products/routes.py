"""/products routes

CRUD endpoints for product processes.
Text search for products.
Etc.
"""

from api.products.router import bp 
from flask import request
from root.savior import Savior
from api.helpers import savior_route, file_to_df, route
from bson import ObjectId
from root.partner import Partner
from typing import Literal
from pymongo import MongoClient

@bp.post("/<string:product_id>/<string:stage_name>/processes")
@savior_route(success_code=201) 
def create_process( 
    savior: Partner, 
    product_id: str, 
    stage_name: Literal["sourcing", "assembly", "processing", "transport"]
) -> ObjectId:
    
    """POST method of /proudcts endpoint
    
    Create a new process for a product

    Path args:
        product_id (str): The product_id to create a process for 
        stage_name (str): The stage to insert the process in
        
    Expected json data:
        activity (str): The process activity
        value (Number): The activity value
        unit_type: (str): The unit type of the activity
        unit (str): The unit of the activity
        process (str): Optional. the process name, defaults to activity
        
    Returns:
        the ObjectId of the created process
    """
    return savior.create_product_process(
        process_data=request.json, product_id=product_id, stage=stage_name
    )
    
@bp.route("processes/<string:process_id>", methods=["PUT", "PATCH"])
@savior_route
def update_product_process(savior: Partner, process_id: str) -> bool: 
    """PUT and PATCH methods for processes/<process_id> endpoint
    
    Updates a product process
    
    Path args: 
        process_id (str): The _id of the process to delete
        
    Expected json:
        value (Number): The activity value
        unit_type: (str): The unit type of the activity
        unit (str): The unit of the activity
        process (str): Optional. the process name, defaults to activity
    
    Returns:
        A boolean denoting whether an update occured or not
    """
    return savior.update_product_process(
        process_id=process_id, process_update=request.json
    )
    
@bp.delete("/processes/<string:process_id>")
@savior_route
def delete_product_process(savior: Partner, process_id: str) -> bool:
    """DELETE method for /processes/<process_id> endpoint
    
    Deletes a product process
    
    Path args: 
        process_id (str): The _id of the process to delete
    
    Returns:
        boolean denoting whether a delete was fulfilled or not
    """
    return savior.delete_product_process(process_id=process_id)

    
@bp.post("/", strict_slashes=False)
@savior_route(success_code=201)
def create_product(savior: Partner) -> ObjectId:
    """POST method to /products endpoint. 
    
    Create a product for the savior!
    
    Expected request.form data:
        file: A file upload; csv or excel accepted currently
        name (str): The product name
    
    Returns:
        The ObjectId of the created product
    """
    return savior.create_product(
        product_data=file_to_df(file=request.files.get("file")),
        product_name=request.form.get("name")
    )
    
@bp.get("/<string:product_id>")
@route(needs_db=True)
def get_product(client: MongoClient, product_id: str) -> dict: 
    """GET method to /products/<product_id>. 
    
    Get a published product
    
    Path args:
        product_id (str): The _id of the product to return
        
    Returns:
        A product dictionary that contains info from product to process level
    """
    return Partner.get_product(
        products_collection=client.spt.products,
        product_id=product_id,
        matches={"published": True}
    )
   
@bp.get("/", strict_slashes=False)
@route(needs_db=True)
def search_products(client: MongoClient) -> dict[str, bool | list]:
    """GET method to /products.
    
    Text search for products collection
    
    Query params:
        limit (int): How many results to return, 0 means all
        skip (int): How many results to skip
        q (str): The search query
    
    Returns: 
        A dict with boolean field has_more, and products field
        containing product level info
    """
    return Savior.collection_text_search(
        collection=client.spt.emission_factors,
        query_params=request.args.to_dict(),
        result_dict_field="products",
        matches={"product_id": {"$exists": True}},
        projections={
            "name": 1,
            "co2e": 1,
            "unit_types": 1,
            "keywords": 1,
            "last_update": 1,
            "product_id": 1,
            "rating": 1,
            "image": 1,
        }
    )

@bp.delete("/<string:product_id>")
@savior_route
def delete_product(savior: Partner, product_id: str) -> bool:
    """DELETE method to products/<product_id>
     
     Delete a product
    
    Path args:
        product_id (str): The product_id to delete from products collection
        
    Returns:
        A boolean denoting if a deletion took place
    """
    return savior.delete_product(product_id=product_id)

# @bp.delete("/<string:product_id>/stars")
# @savior_route
# def star_product(savior: User, product_id: str) -> bool:
#     """DELETE method for products/<product_id>/stars.
    
#     This view will delete a star / 'unstar' a product for a user
    
#     Path args:
#         product_id (str): The id of the product to unstar
#     """
#     return savior.handle_stars(
#         product_id=product_id, 
#         resource="products",
#         delete=True
#     )
    
# @bp.post("/<string:product_id>/stars")
# @savior_route
# def star_product(savior: User, product_id: str) -> bool:
#     """POST method for products/<product_id>/stars.
    
#     This view will star a product for a user
    
#     Path args:
#         product_id (str): The id of the product to unstar
#     """
#     return savior.handle_stars(
#         product_id=product_id, 
#         resource="products",
#         delete=False
#     )