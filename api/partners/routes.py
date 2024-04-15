"""/partners endpoint

Routes for loggin in, creating and getting partners
"""

from api.helpers import (
    route, 
    create_account,
    login, 
    check_email_availability,
)
from api.partners.router import bp 
from pymongo import MongoClient
from flask import request, Response
from bson import ObjectId
from root.partner import Partner

@bp.post("/", strict_slashes=False)
@route(needs_db=True, send_return=False, success_code=201)
def create_partner(client: MongoClient) -> Response:
    """POST method /partners
    
    Create a partner account
    
    Expected json:
        company_name (str): The name of the company
        password (str / character sequence): The password account
        region (str): The region of the company, as an 2-char ISO country code, i.e US
        measurement_categories: The categories & scopes 
            the partner will measure during their program
            
    Returns:
        A Response with the partners's account if creation was successful 
        otherwise a Response with the status code of 409 if a 
        pymongo DuplicateKeyError is raised when trying to 
        create the account
    """
    partner = request.json 
    email = partner["email"]
    company_id = ObjectId()
    return create_account(
        db=client.spt, 
        savior_type="partners",
        account = {
        "company_email": email,
        "company": partner["company_name"],
        "password": partner["password"],
        "email": email,
        "region": partner["region"],
        "username": partner["username"],
        "measurement_categories": partner["measurement_categories"],
        "company_id": company_id,
        "_id":  company_id,
        "role": "company",
        }
    )
    


@bp.post("/login")
@route(
    needs_db=True, send_return=False, success_code=200
)
def partner_login(client: MongoClient) -> Response:
    """POST method /partners/login
    
    Login to a partner account
    
    Expected json:
        email (str): The email of the partner account
        password (str): The password of the partner account
        
    Returns:
        A `Response` containing the account dictionary if login is sucessful
        else a 401 UNAUTHORIZED `Response`
    """
    partner = request.json
    return login(
        collection=client.spt.partners, 
        savior_type="partners",
        password=partner["password"],
        username_or_email=partner["email"],
    )

@bp.get("/emails/<string:email>")
@route(needs_db=True)
def check_email_is_available(client: MongoClient, email: str) -> dict[str, bool | dict]:
    """GET method for /partners/emails/<email>.
    
    Path args:
        email (str): The email to validate.
    
    Returns:
        A dict with boolean field `is_available` 
        indicating if the email is available for usage.
    """
    return check_email_availability(collection=client.spt.partners, email=email)

@bp.get("/<string:partner_id>", strict_slashes=False)
@route(needs_db=True)
def get_partner(client: MongoClient, partner_id: str) -> list | dict:
    """GET method to /partners/<partner_id>
    
    An endpoint to get a partner account and their published products
    
    Path args:
        partner_id (str): The company_id of the partner to retrieve
    """
    return Partner.get_partner(db=client.spt, partner_id=partner_id)
    
@bp.get("/", strict_slashes=False)
@route(needs_db=True)
def get_partners(client: MongoClient) -> list:
    """GET method to /partners
    
    Returns:
        A list of partner account dictionaries
    """
    return list(
        client.spt.partners.find(
            {}, 
            {"name": "$company", "company": 1, "joined": 1, "region": 1, "bio": 1}
        )
    )