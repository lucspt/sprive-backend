"""/tasks routes.

These routes are solely for partners as users don't create tasks.
Used to keep track of climate program and assign actions to take.
These tasks are assigned (inserted to mongo collection) automatically
w.r.t a partner's measurement categories, though they can also
create their own tasks to complete
"""

from api.tasks.router import bp 
from api.helpers import savior_route
from root.partner import Partner
from flask import request
from bson import ObjectId

@bp.get("/", strict_slashes=False)
@savior_route
def get_tasks(savior: Partner) -> list:
    """GET method.
    
    Returns:
        A list of dicts containing the partners tasks
        sorted by creation date and completion
    """

    return savior.get_tasks(query_params=request.args.to_dict())

@bp.post("/", strict_slashes=False)
@savior_route(success_code=201)
def tasks(savior: Partner) -> ObjectId:
    """POST method.
    
    Create a task.
    
    Expected json:
        task (str): The actual task being created, e.g. upload utilities data.
        ???
    
    Returns:
        A Response 201, containing the created task's ObjectId when successful,
        otherwise a Response 401
    """
    return savior.create_task(task_data=request.json)

@bp.get("/<string:task_id>")
@savior_route
def get_task(savior: Partner, task_id: str) -> dict:
    """GET method for /<task_id> endpoint
    
    Get a single task
    
    Path args:
        task_id (str): The id of the task to retrieve
    """
    return savior.db.tasks.find_one({"_id": ObjectId(task_id)})

@bp.patch("/<string:task_id>")
@savior_route
def complete_task(savior: Partner, task_id: str) -> bool:
    """PATCH method for /<task_id> endpoint
    
    Mark a task as complete
    
    Path args:
        task_id (str): The _id of the task to complete
        
    Returns: 
        A boolean indicating whether the task was found when searching for it
    Raises:
        Exception: When a task with the `task_id` does not exist
    """
    return savior.complete_task(task_id=task_id)

@bp.patch("/<string:task_id>/assignees")
@savior_route
def assign_task(savior: Partner, task_id: str) -> bool:
    """PATCH method for /<task_id>/assignees
    
    Assign a task to a partner's 'user'
    
    Path args:
        task_id (str): The _id of the task we are assigning
        
    Expected json:
        assignee (str): The company user to assign the task to
        
    Returns:
        A boolean denoting if the task completion took place
    
    Raises:
        Exception: if the assignee or task does not exist
    """
        
    return savior.assign_task(
        task_id=task_id, assignee=request.json["assignee"]
    )