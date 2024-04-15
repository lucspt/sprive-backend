"""/saviors routes

Endpoints only relevant to a savior of type 'partners'.
See ./routes for more.
"""

from api.saviors.router import bp 
from api.helpers import savior_route
from root.partner import Partner
from flask import request, Response
from api.helpers import send, file_to_df
from bson import ObjectId
from datetime import datetime, timezone

@bp.delete("/logout")
def logout() -> Response:
    """DELETE method for /saviors/logout endpoint
    
    Logout of a partner's account, i.e their token cookies, NOT the tokens themselves
    
    Returns:
        A `Response` with a status code of 204 if successful
        otherwise a 500 status code with the error
    """
    try:
        res = send(status=200, content=True)
        res.set_cookie("csrf_access_token", "", expires=0)
        res.set_cookie("access_token_cookie", "", expires=0)
        return res
    except Exception as e:
        return send(
            status=500, content="Unable to complete request", error=e
        )

@bp.get("/files")
@savior_route(send_return=False)
def get_files(savior: Partner) -> Response:
    """GET method for /saviors/files endpoint
    
    Retrieve a partner's files
    
    Returns:
        A `Response` containing the a list of file dictionaries
    """
    return send(content=savior.files, status=200)

@bp.post("/files")
@savior_route(send_return=False, success_code=201)
def upload_file(savior: Partner) -> Response:
    """POST method of /savior/files endpoint.
    
    Uploads an emission file. Accepted file types are: csv, excel / xls
    
    Note that even if there are multiple files uploaded 
    with a list we only process the first one
    
    Expected request form:
        file[]` or file (FileStorage): the file to upload
    Returns:
        The id of the file uploaded
    """
    print("HEREEE")
    get_file = request.files.get
    file = get_file("file[]") or get_file("file")
    filename = file.filename
    file_df = file_to_df(file, filename)
    print(request.form)
    try:
        response, status = savior.handle_emissions_file(
            file_df=file_df, get_form_field=request.form.get, filename=filename
        ), 200
    except Exception as e:
        response, status = e, 400
    response = send(content=response, status=status)
    return response

@bp.get("/files/<string:file_id>")
@savior_route(send_return=False)
def get_file(savior: Partner, file_id: str) -> Response:
    """GET method for /saviors/files/<file_id>
    
    Get the logs inserted by a file when it was uploaded
    
    Path args:
        file_id (str): the id of the file to retrieve
    
    Query params:
        unprocessed_only: If truthy only return logs 
            where emissions haven't been calculated
        
    Returns:
        A `Response` containg a list of log dictionaries which originated from the file
    """
    return send(
        content=savior.get_file_logs(
            file_id=file_id,
            unprocessed_only=request.args.get("unprocessed-only")
        ), 
        status=200
    )

@bp.get("/products")
@savior_route
def partners_products(savior: Partner) -> list:
    """GET method for /saviors/products endpoint
    
    Query params:
        `published`: If truthy, will only return published products
    
    Returns:
        The requesting partners' products    
    """
    return savior.get_products(
        published_only = bool(request.args.get("published"))
    )

@bp.get("/product-names")
@savior_route
def product_names(savior: Partner) -> list:
    """GET method to saviors/product-names endpoint
    
    Returns:
        A list of the partner's distinct product names 
    """
    return savior.db.products.distinct(
        "name", {"savior_id": savior.savior_id}
    )
    
@bp.delete("/products/<string:product_id>")
@savior_route
def delete_partner_product(savior: Partner, product_id: str) -> bool:
    """DELETE method of /saviors/products/<product_id>
    
    Delete a product and all of its stages, processes
    
    Path args:
        product_id (str): The product_id of the product to delete
    
    Returns:
        A boolean denoting whether the deletion took place
    """
    return savior.delete_product(product_id=product_id)
    
@bp.get("/products/<string:product_id>")
@savior_route
def get_partner_product(savior: Partner, product_id: str) -> dict:
    """GET method of saviors/products/<product_id>
    
    Retreive a product and all its stages, processes
    
    Path args:
        product_id (str): The product_id of the product to get
        
    Returns:
        a dict with the product to process level data
    """
    return savior.get_own_product(product_id=product_id)

@bp.patch("/products/<string:product_id>")
@savior_route(success_code=201)
def update_product(savior: Partner, product_id: str) -> dict:
    """PATCH method saviors/products/<product_id>
    
    Perform product level updates
    
    Note that the only editable field is the product name and keywords. 
    The rest is handled by stage / process specific endpoints.
    
    Path args:
        product_id: The id of the product to get
        
    Returns:
        A dict with the product updates and product_id
    """
    return savior.update_product(
        updates=request.json, product_id=product_id
    )


@bp.post("/published-products")
@savior_route(success_code=201)
def publish_product(savior: Partner) -> bool:
    """POST method for /saviors/published-products
    
    Publish a product
    
    Expected json:
        product_id (str): The product_id of the product to publish
    
    Returns:
        A boolean denoting whether the product was published
    """
    return savior.publish_product(
        product_id=request.json["product_id"]
    )
@bp.delete("/published-products/<string:product_id>")
@savior_route
def unpublish_proudct(savior: Partner, product_id: str) ->  bool:
    """DELETE method for /saviors/published-products/<product_id> endpoint 
    
    Unpublish a product
    
    Args:
        product_id (str): The id of the product to delete
    Path args:
        product_id: The id of the product to unpublish
        
    Returns:
        A bool denoting whether the product was unpublished
    """
    return savior.unpublish_product(product_id=product_id)
    
    
@bp.get("/company-teams")
@savior_route
def company_teams(savior: Partner) -> list:
    """GET method of /saviors/company-teams
    
    Returns:
        A list of the partner's distinct company teams
    """
    
    return savior.db.partners.distinct(
        "team", { "company_id": savior.savior_id }
    )
    
@bp.get("/company-users")
@savior_route
def company_users(savior: Partner) -> list:
    """GET method of /saviors/company-users 
    
    Returns:
        A list of the partner's distinct company users
    """
    return savior.db.partners.distinct(
        "username", { "company_id": savior.savior_id }
    )
    
@bp.get("/company-tree")
@savior_route
def get_company_tree(savior: Partner) -> list:
    """GET method of /saviors/company-tree
    
    Returns:
        All the accounts created under the company
     
    """
    
    return list(
        savior.db.partners.find({"company_id": savior.savior_id}, {"password": 0})
    )
    
@bp.post("/company-tree")
@savior_route(success_code=201)
def invite_user(savior: Partner) -> ObjectId:
    """POST endpoint of /saviors/company-tree 
    
    Invite company users
    
    Expected json:
        role: The role of the user
        username: The user's username
        email: The user's email, (not the company's email)
        password: The password of the user's account
        team (str): Optional. The team to assign the user to
        
    Returns:
        The _id of the created user
    """
    account = request.json
    collection = savior.db.partners
    company_info = collection.find_one(
        {"company_id": savior.savior_id, "role": "company"},
        {"company_id": 1, "company_email": 1, "region": 1, "company": 1, "_id": 0}
    )
    return collection.insert_one(
        {
            **company_info,
            "role": account["role"],
            "password": account["password"],
            "username": account["username"],
            "email": account["email"],
            "joined": datetime.now(tz=timezone.utc),
            "team": account.get("team", None),
        }
    ).inserted_id