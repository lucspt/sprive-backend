from root.savior import Savior
import pytest 
from pytest import fixture
from datetime import datetime, timezone, timedelta
from bson import ObjectId

NUM_PROCESSED_LOGS = 13
NUM_UNPROCCESSED_LOGS = 10
NUM_LOGGED_TODAY = NUM_PROCESSED_LOGS
NOW = datetime.now(tz=timezone.utc)
BEFORE_TODAY =  NOW - timedelta(days=1)

class TestSavior:
    
    @fixture(scope="class")
    def savior_id(self, mock_partner_account):
        return mock_partner_account["_id"]
      
    @fixture(scope="class", autouse=True)
    def setup(self, db, savior_id):
        """We setup a savior account, 
        which is by using a partner id and set up a partner
        and we insert some logs
        """
        unprocessed_logs = [{
            "savior_id": savior_id,
            "source_file": { "upload_date": BEFORE_TODAY }
        } for _ in range(NUM_UNPROCCESSED_LOGS)]
        
        processed_logs = [{
            "savior_id": savior_id, 
            "co2e": 1, 
            "source_file": { "upload_date": NOW }
        } for _ in range(NUM_PROCESSED_LOGS)]
        mock_logs = unprocessed_logs + processed_logs
        db.logs.insert_many(mock_logs)
        yield
        db.logs.delete_many({"savior_id": savior_id})
        
    @fixture(scope="class")
    def savior(db, savior_id):
        savior = Savior(savior_id=str(savior_id))
        yield savior 
        savior._close()
        
    @pytest.mark.parametrize(
        ("query_type", "collection", "filters", "assertion"),
        [
            (
                "find", 
                "logs", 
                {"co2e": {"$exists": False}}, 
                lambda logs: len(logs) == NUM_UNPROCCESSED_LOGS
            ),
            (
                "aggregate", 
                "logs",
                [
                    {"$group": {
                        "_id": None, "co2e": {"$sum": "$co2e"}, 
                        "savior_id": {"$first": "$savior_id"}
                    }}
                ],
                lambda agg_result: agg_result[0]["co2e"] == NUM_PROCESSED_LOGS
            ),
            (
                "aggregate",
                "logs",
                {"throws": "because it is not a list"},
                None,
            ),
            (
                "aggregate",
                "logs",
                [
                    {"$match": {"source_file.upload_date": {"$gt": BEFORE_TODAY.isoformat()}}}
                ],
                lambda res: len(res) == NUM_LOGGED_TODAY
            ),
            (
                "raises",
                "logs",
                [{"this": "shouldraise"}],
                None,
            )
        ]
    )
    def test_get_data(self, query_type, collection, filters, assertion, savior: Savior, savior_id):
        _get_data = lambda: savior.get_data(
            query_type=query_type, collection=collection, filters=filters
        )
        if (not assertion) or (query_type == "raises"):
            with pytest.raises(Exception):
                _get_data()
        else:
            res = _get_data()
            print(res)
            assert assertion(res)
            assert all(x["savior_id"] == savior_id for x in res)

    def test_get_insert(self, savior: Savior):
        res = savior._get_insert({})
        assert res.keys() & {"created_at", "savior_id"}
        created: datetime = res["created_at"]
        assert created.tzinfo == timezone.utc
        
    def test_string_to_date(self, savior: Savior):
        date = datetime.now(tz=timezone.utc)
        iso_date_string = date.isoformat()
        res = savior.string_to_date(date_string=iso_date_string)
        assert res == date
    
    @pytest.mark.parametrize(
        ("requested_fields", "allowed_fields", "is_error"),
        [
            ({"username", "name"}, {"name", "username"}, False),
            ({"username", "name"}, {"name", "error"}, True),
            ({"username"}, {"name", "username"}, False),
        ]
    )
    def test_protect_request_fields(
        self,
        requested_fields: set | dict, 
        allowed_fields: set, 
        is_error: bool,
        savior: Savior,
    ):
        if is_error:
            with pytest.raises(Exception):
                savior.protect_request_fields(
                    requested_fields=requested_fields,
                    allowed_fields=allowed_fields,
                )
        else:
            assert savior.protect_request_fields(
                requested_fields=requested_fields,
                allowed_fields=allowed_fields
            )
        
    @fixture
    def mock_task_id(
        self, savior: Savior, mock_partner_account: dict
    ) -> ObjectId:
        
        _id = savior.db.tasks.insert_one(
            {
                "savior_id": mock_partner_account["_id"], 
                "task": "test",
                "complete": False
            }
        ).inserted_id
        return _id
    
    def test_perform_collection_update(self, savior: Savior, mock_task_id: ObjectId):
        with pytest.raises(Exception):
            err = "not found"
            res = savior._perform_collection_update(
                "tasks", 
                {"_id": "fails"},
                {"$set": {"complete": True}},
                 error_message=err
            )
            assert str(res) == err
        assert savior._perform_collection_update(
            "tasks", 
            {"_id": mock_task_id},
            {"$set": {"complete": True}},
            error_message="Sould not raise"
        )
        assert savior.db.tasks.find_one(
            {"_id": mock_task_id},
            {"complete": 1}
        )["complete"] == True
        
            
    
    @pytest.mark.parametrize(
        ("requested_fields", "fields", "allowed_fields", "required_fields", "raises"),
        [
            ({"name": "mock", "username": "mock"}, {"name", "username"}, None, None, False),
            ({"allowed", "lovelife"}, None, {"allowed", "lovelife"}, {"lovelife"}, False),
            ({"beautiful"}, {"life", "beautiful"}, None, None, True),
            ({"life"}, None, {"amazing", "life"}, {"amazing"}, True),
            ({"amazing", "life"}, None, {"random", "life", "amazing"}, {"amazing", "life"}, False),
            ({"name", "name"}, None, None, None, True)
        ]
    )
    def test_protect_and_require_fields(
        self,
        requested_fields,
        fields,
        allowed_fields,
        required_fields,
        raises,
        savior: Savior
    ):
        test = lambda: (
            savior.protect_and_require_fields(
                requested_fields=requested_fields,
                fields=fields,
                allowed_fields=allowed_fields,
                required_fields=required_fields,
            )
        )
        if raises:
            with pytest.raises(Exception) as e:
                test()
        else:
            test()