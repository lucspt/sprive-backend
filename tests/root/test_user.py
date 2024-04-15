from root.user import User
import pytest
from pytest import fixture
from typing import Literal
from bson import ObjectId

NUM_INSERTS = 20
class TestUser:
    
    @fixture(scope="class")
    def mock_product_id(self):
        return ObjectId()
    
    @fixture(scope="class", autouse=True)
    def create_user(
        self, db, mock_user_account, mock_product_id, mock_partner_account
    ):
        savior_id = mock_user_account["_id"]
        collections = ["product_logs", "stars"]
        for collection in collections:
            db[collection].insert_many([
                {"savior_id": savior_id} for _ in range(NUM_INSERTS)
            ])
        db.emission_factors.insert_one(
            {
                "product_id": mock_product_id, 
                "name": "mock",
                "savior_id": mock_partner_account["_id"],
                "co2e": 123,
                "source": "partners"
            }
        )
        yield
        
        for collection in collections:
            db[collection].delete_many({"savior_id": savior_id})
        
    @fixture(scope="class")
    def user(self, mock_user_account):
        user = User(
            savior_id=str(mock_user_account["_id"])
        )
        yield user
        user._close()
    
    @pytest.mark.parametrize(
        ("limit", "skip", "should_have_more"),
        [
            (NUM_INSERTS - 1, 0, True),
            (0, NUM_INSERTS, False)
        ]
    )
    def test_skip_limit_cursor(self, limit, skip, db, should_have_more, user: User):
        res, has_more = user.skip_limit_cursor(
            db.product_logs.find({"savior_id": user.savior_id}),
            limit=limit, skip=skip
        )
        assert isinstance(res, list)
        print(res, has_more)
        assert has_more == should_have_more
    
    def __test_collection_inserts(self, collection: Literal["starred_products", "logs"], user):
        _get_inserts = lambda limit: getattr(
            user, collection
        )(limit=limit)
        res = _get_inserts(limit=0)
        assert res["has_more"] == False
        assert len(list(res.values())[0]) == NUM_INSERTS
        res = _get_inserts(limit=NUM_INSERTS - 10)
        assert res["has_more"] == True 
        
    def test_logs(self, user):
        self.__test_collection_inserts(collection="logs", user=user)

    def test_starred_products(self, user):
        self.__test_collection_inserts(collection="starred_products", user=user)
    
    @pytest.mark.parametrize(
        ("invalid_product_id_error", "delete"),
        [(False, True), (False, False), (True, False)]
    )
    def test_handle_stars(
        self, 
        invalid_product_id_error: bool, 
        delete: bool, 
        user: User, 
        mock_product_id,
        db
    ):
        if invalid_product_id_error == True:
            with pytest.raises(Exception):
                res = user.handle_stars(product_id="RANDOMNONEXISTANT")
                assert "does not exist" in str(res)
        else:
            user.handle_stars(product_id=mock_product_id, delete=delete)
            find = {"resource_id": mock_product_id, "savior_id": user.savior_id}
            if delete:
                assert db.stars.find_one(find) is None
            else:
                assert db.stars.find_one(find)
    
    def test_log_product_emissions(self, mock_product_id: ObjectId, user: User, db):
        with pytest.raises(Exception): 
            res = user.log_product_emissions(
                product_id="fails", value=10
            )
            assert "does not exist" in str(res)
        test_value = 2
        res = user.log_product_emissions(product_id=mock_product_id, value=test_value)
        assert isinstance(res, dict)
        product_co2e = db.emission_factors.aggregate([
            {"$match": {"product_id": mock_product_id}},
            {"$project": {"co2e": 1}}
        ]).next()["co2e"]
        assert res["co2e"] == product_co2e * test_value
      
    @pytest.mark.parametrize(
        ("user_method", "bool_assertion"),
        [("start_spriving", True), ("stop_spriving", False)]
    )          
    def test_start_and_stop_spriving(
        self, user_method: str, bool_assertion: bool, user: User, db
    ):
        getattr(user, user_method)()
        assert user.db.users.find_one(
            {"_id": user.savior_id}, {"spriving": 1}
        )["spriving"] == bool_assertion
        
    
                