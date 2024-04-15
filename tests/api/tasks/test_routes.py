from bson import ObjectId

def test_tasks_get(partner_auth, assert_route):
    assert_route("/tasks", "get", partner_auth, list)
    
def test_tasks_post_get_patch_asssign(partner_auth, assert_route, mock_partner_account):
    task_id = assert_route(
        "/tasks", 
        "post",
        partner_auth,
        ObjectId, 
        json={"task": "randomtask", "savior_id": mock_partner_account["_id"]}
    )
    task_route = f"/tasks/{task_id}"
    task = assert_route(task_route, "get", partner_auth, dict)
    assert task
    assert_route(task_route, "patch", partner_auth, int)
    
    assert_route(
        f"{task_route}/assignees", 
        "patch", 
        partner_auth, 
        bool, 
        json={"assignee": "LOVELIFE"}
    )