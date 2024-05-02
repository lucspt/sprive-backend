import json
from tests.utils import decode_response

def test_tasks_get(partner_auth, assert_route):
    assert_route("/tasks", "get", partner_auth, list)
    
def test_tasks_post_get_patch_asssign(partner_auth, assert_route, db, mock_partner_account):
    __insert_task = {
        "task": "random_task",
        "savior_id": mock_partner_account["_id"]
    }
    task_id = db.tasks.insert_one(__insert_task).inserted_id
    task_route = f"/tasks/{task_id}"
    task = assert_route(task_route, "get", partner_auth, dict)
    assert task == json.loads(json.dumps(__insert_task, default=str))
    assert_route(task_route, "patch", partner_auth, int)
    assert_route(
        f"{task_route}/assignees", 
        "patch", 
        partner_auth, 
        bool, 
        json={"assignee": "LOVELIFE"}
    )