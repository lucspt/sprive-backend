from root.partner import Partner
import pytest
from pytest import fixture
from bson import ObjectId
from datetime import datetime, timedelta
from numbers import Number
import pandas as pd
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timezone
            
@fixture(scope="class")
def savior_id(mock_partner_account):
    return mock_partner_account["_id"]
    
@fixture(scope="class")
def partner(savior_id):
    savior_id = str(savior_id)
    partner = Partner(
        savior_id=savior_id,
        user_id=savior_id
    )
    yield partner
    partner._close()
    
@fixture(scope="class", autouse=True)
def setup(db, savior_id):
    _id = db.tasks.insert_one(
        {
            'savior_id': savior_id,
            'created': datetime.now(tz=timezone.utc),
            'task': 'Test task',
            'complete': False,
            'category': 'Testing',
            'assignee': 'testing 1712689697.495591',
            'type': 'test',
            'action': 'testing'
        }
    ).inserted_id
    yield
    db.tasks.delete_one({"_id": _id})
    
@fixture(scope="class", autouse=True)
def delete_test_inserts(partner: Partner, savior_id):
    yield
    for collection in ["logs", "products", "tasks"]:
        partner.db[collection].delete_many({"savior_id": savior_id})
    
class TestPartner:    
        
    
    def test_savior_property(self, partner: Partner, savior_id):
        """Test `savior` property returns correct savior_id"""
        assert partner.savior["savior_id"] == savior_id
        
    def test_update_profile(self, partner: Partner):
        """
        Test the update of a profile. we test username and make sure that when it is changed,
        all tasks assigned to that username will be changed as well
        """
        new_username = f"testing {datetime.now().timestamp()}"
        def get_assigned_tasks(username):
            return list(partner.db.tasks.find(
                {"savior_id": partner.savior_id, "assignee": username }
            ))
        tasks_before = get_assigned_tasks(username=partner.savior["username"])
        partner.update_profile({"username": new_username})
        assert partner.savior["username"] == new_username
        if (tasks_before):
            tasks_after = get_assigned_tasks(new_username)
            assert bool(tasks_after)
    
    def test_process_file_logs(self, partner: Partner, savior_id):
        mock_logs = [
            {
                "activity": "test",
                "category": "test",
                "source_file": {
                    "name": "test.csv"
                }
            
            }
        ]
        mock_task_id = partner.db.tasks.find_one(
            {"savior_id": savior_id, "complete": False}, {"_id": 1}
        )["_id"]
        file_id = partner.process_file_logs(mock_logs, task_id=str(mock_task_id))
        inserted_logs = list(
            partner.db.logs.find(
                {"savior_id": savior_id, "source_file.id": file_id}
            )
        )
        assert inserted_logs
        now = datetime.now()
        # for some reason pymongo doesn't return a datetime with tzinfo of timezone.utc 
        # even though we insert it as so, but this is passing anyways
        for x in inserted_logs:
            assert isinstance(x.get("co2e"), Number)
            source_file = x["source_file"]
            upload_date = source_file.get("upload_date")
            assert now - upload_date <= timedelta(minutes=5)
        assert partner.db.tasks.find_one({"_id": mock_task_id})["complete"] == True
        partner.db.tasks.update_one({"_id": mock_task_id}, {"$set": {"complete": False}})
    
    def test_get_files(self, partner: Partner):
        files = partner.files
        assert files
        for file in files:
            file_id = file["_id"]
            file_to_check = file
            assert file.keys() & {"_id", "needs_processing", "co2e", "upload_date"}
        file_logs = partner.get_file_logs(file_id)
        emissions = [x.get("co2e") for x in file_logs]
        if file_to_check["needs_processing"]:
            assert any(not co2e for co2e in emissions)
        else:
            assert all(isinstance(x, Number) for x in emissions)
        assert sum(emissions) == file_to_check["co2e"]
    
    @fixture(scope="class")
    def mock_product_name(self):
        return "test product"

    @fixture(scope="class", autouse=True)
    def mock_product_id(self, partner: Partner, mock_product_csv, mock_product_name):
        product_id = partner.create_product(
            product_data=mock_product_csv,
            product_name=mock_product_name
        )
        return product_id
     
    def test_create_product(
        self, 
        partner: Partner, 
        mock_product_csv: pd.DataFrame,
        mock_product_name: str,
    ):
        assert len(partner.get_products()) > 0
        with pytest.raises(Exception) as err:
            fail_product = mock_product_csv.copy()
            fail_product.loc[:, "stage"] = "should_fail"
            partner.create_product(product_data=fail_product, product_name="fail")
            assert str(err).lower() == "invalid stage name"
        with pytest.raises(Exception):
            partner.create_product(
                product_data=mock_product_csv, 
                product_name=mock_product_name
            )
            assert "product names must be unqiue" in str(err)
    
    def test_get_products(self, partner: Partner, db):
        def _assert_keys_exist(products_list):
            assert all(
                x.keys() & {
                    "_id",
                    "co2e",
                    "keywords",
                    "category",
                    "product_id",
                    # "rating",
                    "created_at",
                    "last_update",
                    "name",
                } for x in products_list
            )
        products = partner.get_products()
        _assert_keys_exist(products_list=products)
        published_only_products = partner.get_products(published_only=True)
        _assert_keys_exist(products_list=published_only_products)
        num_published_products = len(
            db.products.distinct(
                "product_id",
                {"savior_id": partner.savior_id, "published": True}
            )
        )
        assert len(published_only_products) == num_published_products 
        
    def test_get_own_product(
        self, 
        partner: Partner, 
        mock_product_id: ObjectId, 
        mock_product_csv: pd.DataFrame
    ):
        """Test the return of a product aggregation"""
        stage_names = mock_product_csv["stage"].unique()
        process_names = mock_product_csv["activity"].unique()
        stage_names, process_names, processes, stages = [], [], [], []
        product = partner.get_own_product(product_id=mock_product_id)
        print(product, "HERE")
        for stage in product["stages"]:
            assert stage.keys() & {
                "co2e", "stage", "num_processes", "processes"
            }
            stage_name = stage["stage"]
            assert stage["num_processes"] == len(mock_product_csv[
                mock_product_csv["stage"] == stage_name
            ]["activity"])
            stages.append(stage)
            stage_names.append(stage_name)
            for process in stage["processes"]:
                assert process.keys() & {
                  "activity",  
                  "activity_unit_type", 
                  "activity_value", 
                  "last_update", 
                  "keywords",
                  "co2e"
                }
                processes.append(process)
                process_name = process["process"]
                process_names.append(process_name)
        assert mock_product_csv["stage"].isin(stage_names).all()
        assert mock_product_csv["activity"].isin(process_names).all()
        product_co2e = product["co2e"]
        assert sum(stage["co2e"] for stage in stages) == product_co2e
        assert sum(
            process["co2e"] for process in processes
        ) == product_co2e
        
    def test_publish_and_unpublish_product(
        self, 
        partner: Partner, 
        mock_product_id: ObjectId,
    ):
        def _is_product_published() -> bool:
            return partner.db.products.find_one(
                {
                    "published": True, 
                    "product_id": mock_product_id, 
                    "savior_id": partner.savior_id
                }
            )
            
        assert not _is_product_published()
        partner.publish_product(product_id=mock_product_id)
        assert _is_product_published()
        partner.unpublish_product(product_id=mock_product_id)
        assert not _is_product_published()
    
    
    def test_update_product(self, 
        partner: Partner,
        mock_product_id: ObjectId, 
        mock_product_name: str
    ):
        # with pytest.raises(Exception):
        #     partner.update_product(
        #         {"name": mock_product_name}, 
        #         product_id=mock_product_id
        #     )
        # with pytest.raises(Exception):
        #     partner.update_product(
        #         {"name": "random"},
        #         product_id=ObjectId(),
        #     )
        mock_update = {"name": "love life    ", "keywords": "123     "}
        assert partner.update_product(mock_update, product_id=mock_product_id)