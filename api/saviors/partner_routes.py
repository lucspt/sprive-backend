from api.saviors.router import bp 
from api.helpers import savior_route
from root.saviors.partner import Partner
from flask import request, Request, Response
from api.helpers import send, file_to_df
import pandas as pd
import numpy as np 
from bson import ObjectId
from flask_mail import Message
import sys 

def handle_emissions_file(savior: Partner, request: Request) -> Response:
    """This will upload file contents from a partner as `logs` to
    the `logs` collection, checking and adding the information needed"""
    file = request.files.get("file[]")
    filename = file.filename
    data = file_to_df(filename=filename, file=file)
    missing_columns = []
    for required_col in ["activity", "category", "value", "unit"]:
        if required_col not in data:
            missing_columns.append(required_col)
    if len(missing_columns) > 0:
        send(content=f"Missing required columns: {", ".join(missing_columns)}", status=400)
    data.loc[:, "source_file"] = [
        {"name": filename, "size": sys.getsizeof(file), "processed": False}
    ] * len(data)
    documents = data.replace({np.nan: None}).to_dict("records")
    savior.handle_file_logs(file_logs=documents)
    return send(content={"filename": file.filename}, status=200)

@bp.route("/files/<string:file_id>", methods=["GET"])
@bp.route("/files", methods=["GET", "POST", "PUT"])
@savior_route(send_return=False)
def files(savior: Partner, file_id: str = None) -> list:
    """Upload, view uploaded, and calculate the emissions of files"""
    method = request.method 
    if method == "GET":
        processing_only = request.args.get("processing-only", False)
        response = send(
            content=savior.get_files(
                file=file_id, processing_only=processing_only
            ), status=200
        )
    elif method == "POST":
        return handle_emissions_file(savior=savior, request=request)
    elif method == "PUT":
        response = send(
            content=savior.calculate_file_emissions(request.json["data"]),
            status=200
        )
    return response 

@bp.route("/suppliers", methods=["GET", "POST", "PUT"])
@savior_route
def suppliers(savior: Partner) -> ObjectId:
    """Get suppliers of a savior, add new suppliers"""
    method = request.method
    if method == "GET":
        return savior.suppliers
    elif request.method == "PUT":
        return savior.add_supplier(request.json)

@bp.route("/suppliers/<string:id>")
@savior_route
def supplier(savior: Partner, id: str) -> dict:
    """Access to a supplier and the ability to message them"""

    method = request.method 
    if method == "GET":
        response = savior.db.suppliers.find_one({"_id": ObjectId(id)})
    elif method == "POST":
        # send mail
        supplier = None
        msg = Message( 
            subject="Carbon footprint management",
            recipients=supplier["email"]
        )
        # from app import mail 
        # mail.send(msg)
    return response 

@bp.get("/products/names")
@savior_route
def product_names(savior: Partner) -> list:
    """Get distinct product names. 
    This is used to verify a product name is unique when creating one
    """
    return savior.db.products.distinct(
            "name", {"savior_id": savior.savior_id}
    )
    
@bp.route("/products/<string:id>", methods=["GET", "DELETE", "PATCH"])
@savior_route
def product(savior: Partner, id: str) -> dict | bool:
    """Handles products generally,
    since we are using references, we use `many` updates / deletions
    """
    method = request.method 
    if method == "GET": 
        response = savior.get_product(product_id=id) 
        print(response, "RESSSS")
    elif method == "DELETE":
        response = savior.db.products.delete_many(
            {"product_id": id}
        ).acknowledged
    elif method == "PATCH":
        update = request.json
        name = update.get("name")
        if name:
            name = name.strip()
            update["name"] = name
            update["activity"] = name
        kwds = update.get("keywords")
        if kwds:
            update["keywords"] = kwds.strip()
        response = savior.db.products.update_many(
            {"product_id": id}, {"$set": update}
        ).acknowledged
    return response

# @bp.route("/products/processes", methods=["POST"])
# @bp.route("/products/processes/<string:id>", methods=["DELETE", "POST", "PUT"])
# @savior_route
# def product_processes(savior: Partner, id: str | None = None) -> bool | ObjectId:
#     """Handles product processes; the actual emission causing 
#     aspects of products"""
#     method = request.method
#     if method == "DELETE":
#         response = savior.db.products.delete_one(
#             {"_id": ObjectId(id)}
#         ).acknowledged
#     elif method == "PUT": 
#         print(id, "IDDDD")
#         process_update = request.json 
#         response = savior.handle_product_processes(
#             id=id, 
#             process_update=process_update, 
#             calculate_emissions=process_update.pop("calculate_emissions", False)
#         )         
#     elif method == "POST":
#         response = savior.handle_product_processes(
#             process_update=request.json
#         )
#     return response 

@bp.route("/published-products", methods=["POST"])
@bp.route("/published-products/<string:product_id>", methods=["DELETE"])
@savior_route
def handle_product_publishings(
    savior: Partner, product_id: str | None = None
) -> bool:
    """This endpoint takes a product id handles publishing and unpublishing products"""
    method = request.method 
    if method == "POST":
        response =  savior.publish_product(
            product_id=request.json["product_id"]
        )
    elif method == "DELETE":
        response = savior.unpublish_product(product_id=product_id)
    return response



@bp.route("/tasks/<string:task_id>", methods=["GET", "PATCH", "PUT", "DELETE"]) 
@bp.route("/tasks", methods=["GET", "POST"])
@savior_route
def tasks(savior: Partner, task_id: str = None) -> dict | ObjectId | int:
    """get and create new tasks / todos, basically a todo list endpoint"""
    tasks = savior.db.tasks
    method = request.method
    if method == "GET":
        if task_id:
            res = list(tasks.find_one({"_id": ObjectId(task_id)}))
        else:
            savior_id = savior.savior_id
            res = {
                "pending": tasks.count_documents({
                    "savior_id": savior_id, "status": "in progress"
                }),
                "tasks": list(
                    tasks.find(
                        {"savior_id": savior_id}, 
                        sort=[("status", -1), ("created", -1)]
                    )
                )
            }
    elif method == "PUT":
        json = request.json 
        json.pop("_id")
        res = tasks.update_one(
        {"_id": ObjectId(task_id)}, {"$set": request.json}
    ).upserted_id
    elif method == "POST":
        json = request.json 
        json.pop("_id")
        savior = savior 
        res = savior._get_insert(
            {**request.json, "status": "in progress"}
        )
        tasks.insert_one(res)
    elif method == "PATCH": 
        res = tasks.update_one(
            {"_id": ObjectId(task_id)},
            {"$set": {"status": "complete"}}
        )
    elif method == "DELETE":
        res = tasks.delete_many({"_id": ObjectId(task_id)}).deleted_count
    return res

@bp.route("/factors/bookmarks/<string:factor_id>", methods=["DELETE", "PATCH"])
@savior_route
def handle_factor_bookmars(savior: Partner, factor_id: str) -> str:
    """Add and remove emission factor bookmarks"""
    actions = {"PATCH": "$addToSet", "DELETE": "$pull"}
    accumulator = actions[request.method]
    return savior.handle_emission_factor(
        factor_id=factor_id, accumulator=accumulator
    )

@bp.get("/emissions")
@savior_route
def emissions(savior: Partner) -> list: 
    """get logged emissions of a savior"""
    return savior.emissions 

