import api.saviors.user_routes
import api.saviors.partner_routes
from flask import request
from typing import Literal
from api.helpers import send
from api.saviors.router import bp
from bson import ObjectId
from celery.result import AsyncResult
from api.helpers import savior_route
from root.saviors.partner import Partner
from root.saviors.user import User
# from flask_mail import Message
# from openai import OpenAI

@bp.patch("/", strict_slashes=False)
@savior_route
def update_profile(savior: User | Partner):
    return savior.update_profile(request.json)

@bp.get("/", strict_slashes=False)
@savior_route
def get_savior(savior: User | Partner):
    return savior.savior

@bp.route("/data/<string:data>", methods=["GET"])
@savior_route
def get_dashboard(
    savior: Partner | User,
    data: Literal[
        "overview", 
        "products",
        "pledges",
        "stats",
        "suppliers"
    ],
) -> dict:
    """An endpoint that returns dashboard sections for the frontend"""
    return getattr(savior, data)

@bp.post("/emissions")
@savior_route
def get_emissions_status(savior: User) -> dict:
    """This endpoint retrieves the emissions status for a user
    given the time frames requested in the json body
    
    It provides a way for the front end to get the stats info it needs
    to display to the user
    """
    emissions = savior.emissions
    if callable(emissions):
        return savior.emissions(date_ranges=request.json)
    else:
        return savior.emissions
    
@bp.route("/data", methods=["POST"])
@savior_route 
def handle_data(savior: Partner | User) -> list | dict:
    """Access to db `find` and `aggregate` methods"""
    json = request.json
    res = savior.get_data(**json)
    return res  

@bp.route("/pledges", methods=["POST", "GET"])
@savior_route
def create_pledge(savior: Partner | User) -> ObjectId:
    """Post new pledges"""
    if request.method == "GET":
        return savior.pledges
    else:
        pledge_info = request.json
        response, is_recurring = savior.make_pledge(pledge_document=pledge_info)
        if is_recurring:
            print("START CELERY BEAT HERE")
        return response
    
@bp.route("/", methods=["POST", "GET"], strict_slashes=False)
@savior_route
def products(savior: Partner) -> list | ObjectId:
    """Create new products with this endpoint.
    
    The product stages can get bulky, so even though we are duplicating data,\n
    we have chosen to use references instead of embedding documents
    """
    method = request.method 
    if method == "GET":
        response = savior.products
    elif method == "POST":
        """for when a product is first created, and has no stages yet, 
        we use this endpoint to get an id, this allows it to be created
        with the structure we're using where it's actually a collection of 
        individual processes
        """
        response = ObjectId() 
    return response

@bp.route("/pledges/<string:id>", methods=["GET", "PUT"])
@savior_route
def pledge(savior: User | Partner, id: str) -> dict | bool:
    """Specific pledges of a savior, for getting and updating"""
    method = request.method 
    id = ObjectId(id)
    if method ==  "GET":
        response = savior.db.pledges.find_one({"_id": id})
    elif method == "PUT":
        response = savior.db.pledges.update_one(
            {"_id": id}, {"$set": request.json}
        ).acknowledged
    return response  

@bp.get("/products")
@savior_route
def savior_products(savior: Partner):
    return savior.products
     
    
# @bp.route("/threads/<string:thread_id>", methods=["GET", "POST"])
# @savior_route
# def chat(savior: User | Partner, thread_id: str) -> list:
#     """This endpoint takes a thread id (a thread of messages)
#     and can either return it's messages if the request method is get, 
#     or get a new chat completion from gpt and insert into db for usage later
#     """
#     method = request.method
#     client = OpenAI()
#     if method == "POST":
#         prompt = {"role": "user", "content": request.json["text"]}
#         query_insert = savior._get_insert(prompt)
#         response = client.chat.completions.create(
#             model="gpt-3.5-turbo",
#             messages=prompt,
#             stream=True,
#         )
#         chat_completion = []
#         def stream_response():
#             for chunk in response:
#                 chunk_completion = chunk.choices[0]
#                 if (completion_text := chunk_completion.delta.content):
#                     chat_completion.append(completion_text)
#                     yield completion_text
#                 elif chunk_completion.finish_reason == "stop":
#                     savior.db.threads.insert_many(
#                         [
#                             query_insert,
#                             savior._get_insert(
#                                 {"role": "assistant", "content": chat_completion.join(" ")}
#                             )
#                         ]
#                     )
#         return stream_response(), 200
#     elif method == "GET":
#         return savior.get_thread(thread_id=thread_id)
    
from queues.tasks import add    
@bp.route("/test-celery", methods=["POST"])
def test_celery():
    try: 
        json = request.json 
        a, b = json["a"], json["b"]
        result = add.delay(a, b)
        return send(content=result.id, status=200)
    except Exception as e:
        return send(content=e, status=400)
    
@bp.get("/test-celery/<string:id>")
def get_celery(id: str):
    result = AsyncResult(id)
    content = {
        "ready": result.ready(),
        "sucessful": result.successful(),
        "value": result.get() if result.ready() else None
    }
    return send(content=content, status=200)


@bp.route("/stars/<string:product_id>", methods=["POST", "DELETE"])
@savior_route
def star_product(savior: User, product_id: str) -> bool:
    if request.method == "POST":
        return savior.handle_stars(
            id=product_id, 
            delete=False,
            product_info=request.json 
        )
    else:
        return savior.handle_stars(id=product_id, delete=True)
    