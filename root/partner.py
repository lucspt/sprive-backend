"""CRUD operations requested by partners"""

from root.savior import Savior
from bson import ObjectId
from typing import Literal, override, Any
from datetime import datetime, timezone
from root.emissions import GHGCalculator
from pandas import DataFrame
import numpy as np
from werkzeug.datastructures import ImmutableMultiDict
from pymongo.collection import Collection
from pymongo.database import Database
from exceptions import (
    InvalidRequestDataError,
    ResourceConflictError,
    MissingRequestDataError,
    ResourceNotFoundError
)


class Partner(Savior):
    """See base `Savior` class for more
    
    
    Attributes:
        savior (dict): The account information of current savior making requests. 
            This is the company_id of the partner, rather than the account's _id,
            and does not contain sensitive fields like password
        current_user_id (str): The current_user_id which is the _id field
            of the requesting account. Note that this is not in ObjectId form 
            to reduce unneccesary function calls as this usually does not need
            to be accessed. If used, convert to ObjectId first.
        files (list): The file-level infomration of uploaded files
        all_product_stages (set): The possible names of a product,
            which are also the stages required in order to publish a product
        """

    __slots__ = ("current_user_id")
    
    @override
    def __init__(self, savior_id: str, user_id: str):
        super().__init__(savior_id=savior_id)
        self.current_user_id = user_id
        
        
        
    @property
    def savior(self) -> dict:
        return self.db.partners.find_one(
            {"_id": ObjectId(self.current_user_id)}, 
            {
                "savior_id": "$company_id", 
                "user_id": "$_id", 
                "role": 1, 
                "_id": 0,
                "username": 1, 
                "team": 1, 
                "joined": 1,
                "email": 1,
                "company": 1,
                "company_email": 1,
                "measurement_categories": 1,
                "region": 1
            }
        )            
    
    def update_profile(self, updates: dict) -> bool:
        """Update the profile
        
        The fields allowed to update currently include:
            - username
            - name
            - password
            - email
            - measurement_categories
        
        Args:
            updates (dict): A dictionary of all updates to perform
        
        Returns:
            A bool indicating if the update was successful
        
        Raises:
            Exception: When a requested update field is immutable or invalid
        """
        
        self.protect_request_fields(
            updates, {"username", "name", "password", "email", "measurement_categories"}
        )
        savior_id = self.savior_id
        new_username = updates.get("username")
        if new_username:
            old_username = self.savior["username"]
            self.db.tasks.update_many(
                {"savior_id": savior_id, "assignee": old_username},
                {"$set": {"assignee": new_username}}
            )
        return bool(
            self.db.partners.update_one(
                {"_id": savior_id}, {"$set": updates}
            ).modified_count
        )
        
    def logs(self, limit: int = 0, skip: int = 0) -> list:
        """Partner's logs
        
        Args:
            limit (int): Limit the results returned
            skip (int): Skip results before limiting
            
        Returns:
            A list of log documents
        """
        print(self.savior_id)
        return list(
            self.db.logs
            .find({"savior_id": self.savior_id})
            .sort([("created_at", -1)])
            .skip(skip)
            .limit(limit)
        )
        
    # def calculate(
    #     self,
    #     activity_id: str,
    #     activity_value: Number,
    #     activity_unit: str,
    #     activity_unit_type: str,
    #     activity: str | None = None
    # ) -> dict:
    #     "emissions calculation"
    #     ghg_calculator = GHGCalculator(region=self.savior["region"] or "US")
    #     emissions = ghg_calculator(
    #         activity_id=activity_id,
    #         value=activity_value,
    #         unit=activity_unit,
    #         unit_type=activity_unit_type
    #     )
    #     if activity: emissions["activity"] = activity
        
    #     return {
    #         **emissions, 
    #         "activity_unit_type": activity_unit_type,
    #         "activity_unit": activity_unit,
    #         "activity_value": activity_value,
    #         "activity_id": activity_id,
    #         "tool_call_query": None, #this comes from amulet only
    #     }
    

    def get_file_logs(self, file_id: str, unprocessed_only: Any = False) -> list:
        """Retrieve the logs uploaded by a file
        
        Args:
            file_id (str): The id of the file to get logs from
            unprocessed_only (bool): If truthy, return only the 
                logs where emission haven't been calculated
        """
        if unprocessed_only:
            return list(
                self.db.logs.find({
                    "savior_id": self.savior_id,
                    "source_file.id": ObjectId(file_id),
                    "co2e": {"$exists": False}
                })
            )
        else:
            return list(
                self.db.logs.aggregate([
                {
                    "$match": {
                        "savior_id": self.savior_id, 
                        "source_file.id": ObjectId(file_id)
                    }
                },
                {
                    "$addFields": {
                        "processed": {"$gt": ["$co2e", None]}
                    }
                },
                {"$sort": {"processed": 1, "created_at": -1}}
            ])
        )
    
    @property
    def files(self) -> list:
        """Get the uploaded files of a partner
        
        This will not return the logs. This method retrieves documents
        with file-level information such as name, id, and needs_processing
        
        Note: To get the logs of a partner use the logs method
        
        Returns:
            a list of files with an added boolean field named needs_processing
        """
        return list(
            self.db.logs.aggregate([
                {"$match": {"savior_id": self.savior_id}},
                {
                    "$addFields": {
                        "needs_processing": {
                            "$lt": ["$co2e", None]
                        }
                    }
                },
                {
                    "$group": {
                        "_id": "$source_file.id",
                        "needs_processing": {
                            "$addToSet": "$needs_processing"
                        },
                        "co2e": {"$sum": "$co2e"},
                        "upload_date": {"$first": "$source_file.upload_date"},
                    }
                },
                {
                    "$addFields": {
                        "needs_processing": {
                            "$anyElementTrue": ["$needs_processing"]
                        }
                    },
                },
                {"$sort": {"needs_processing": 1}}
            ])
        )
        
    def get_products(self, published_only: bool = False) -> list:
        """Get created products
        
        Args:
            published_only: If truthy only return published products
        
        Returns:  
            A list of products, with their processes grouped by their stages
        """
        _match = {"savior_id": self.savior_id}
        if published_only: _match["published"] = True
        return list(self.db.products.aggregate(
            [
                {"$match": _match},
                {
                    "$group": {
                        "_id": "$product_id", 
                        "co2e": {"$sum": "$co2e"},
                        "keywords": {"$first": "$keywords"},
                        "category": {"$first": "$category"},
                        "product_id": {"$first": "$product_id"},
                        "rating": {"$first": "$rating"},
                        "created_at": {"$first": "$created"},
                        "last_update": {"$first": "$last_update"},
                        "image": {"$first": "$image"},
                        "name": {"$first": "$name"}
                    }
                },
                {"$sort": {"last_update": -1, "created_at": -1}}
            ]
        ))
        
    @staticmethod
    def get_product(
        products_collection: Collection, product_id: str, matches: dict = {}
    ) -> dict:
        """Get a product, its stages and processes.
        
        Perform an aggregation on products collection to return 
        high level info from product, such as name last_update, etc,
        as well as stage and process level info like activity, unit, etc.
        
        Args:
            products_collection (pymongo.Collection): The mongodb products collection
                to perform the aggregation upon.
            product_id (str): The product_id of the product to get.
            matches (dict): A dictionary containing any filters to 
                add to the $match stage of the aggregation pipeline.
            
        Returns:
            All the product's processes, emissions, etc    
        """
        res = products_collection.aggregate([
            {"$match": {**matches, "product_id": ObjectId(product_id)}},
            {
                "$group": {
                    "_id": "$stage",
                    "co2e": {"$sum": "$co2e"},
                    "keywords": {"$first": "$keywords"},
                    "published": {"$first": "$published"},
                    "last_update": {"$first": "$created"},
                    "name": {"$first": "$name"},
                    "product_id": {"$first": "$product_id"},
                    "unit_types": {"$first": "$unit_types"},
                    "activity": {"$first": "$activity"},
                    "stars": {"$first": "$stars"},
                    "image": {"$first": "$image"},
                    "processes": {"$push": {
                    "_id": "$_id",
                    "process": "$process",
                    "activity": "$activity",
                    "activity_id": "$activity_id",
                    "activity_unit": "$activity_unit",
                    "activity_unit_type": "$activity_unit_type",
                    "activity_value": "$activity_value",
                    "co2e": "$co2e"
                    }},
                },
            },
            {
                "$group": {
                    "_id": None, 
                    "stages": {
                        "$push": {
                            "co2e": {"$sum": "$processes.co2e"},
                            "num_processes": {"$size": "$processes"},
                            "stage": "$_id",
                            "processes": "$processes",
                            "last_update": "$last_update",
                        }
                    },
                    "image": {"$first": "$image"},
                    "co2e": {"$sum": "$co2e"},
                    "published": {"$first": "$published"},
                    "unit_types": {"$first": "$unit_types"},
                    "product_id": {"$first": "$product_id"},
                    "activity": {"$first": "$activity"},
                    "name": {"$first": "$name"},
                    "stars": {"$first": "$stars"},
                    "keywords": {"$first": "$keywords"}
                }
            }
        ])
        return res.next() if res.alive else {}

    def get_own_product(self, product_id: str) -> dict:
        """Get a product.
        
        Same functionality as `get_product`, yet only requires product_id,
        since it is not a static method. It also matches on savior_id equality
        for use of mongodb indexing
        
        See `get_product` for more
        """
        p = self.get_product(
            products_collection=self.db.products,
            product_id=product_id,
            matches={"savior_id": self.savior_id}
        )
        return p
    
    
    def delete_product_process(self, process_id: str) -> bool:
        """Delete a process from a product stage
        
        Args:
            process_id (str): The _id of the process
        """
        return bool(
            self.db.products.delete_one(
                {"_id": ObjectId(process_id)}
            ).deleted_count
        )
    
    @staticmethod
    def calculate_emissions() -> dict[str, int]:
        import random
        return {"co2e": random.randint(0, 5)}
    
    @classmethod
    def protect_process_request(self, process: dict | set):
        # required_fields = {"co2e", "activity", "value", "unit", "unit_type"}
        # self.protect_and_require_fields(
        #     requested_fields=process, 
        #     allowed_fields=required_fields & {"scope"},
        #     required_fields=required_fields
        # )
        return
        
    def create_product_process(
        self, 
        product_id: str, 
        stage: Literal["sourcing", "assembly", "processing", "transport"], 
        process_data: dict,
    ) -> ObjectId:
        """Create a product process
        
        Add a new process to an existing product given a stage name
        
        
        Args:
            product_id (str): The product to create the process under
            stage (str): The stage to insert the process into
            process_data (dict): The process's data
        
        Returns: 
            the ObjectId of the process
        """
        import random 
        if stage not in self.all_product_stages:
            raise InvalidRequestDataError("Invalid stage name")
        self.protect_process_request(process=process_data)
        process_data["co2e"] = random.randint(0, 4)
        now = datetime.now(tz=timezone.utc)
        return self.db.products.insert_one(
            {
                "product_id": ObjectId(product_id), 
                "stage": stage, 
                **process_data, 
                "savior_id": self.savior_id,
                "last_update": now,
                "created_at": now,
                "name": process_data["process"],
                
            }
        ).inserted_id
        
    def update_product_process(self, process_id: str, process_update: dict) -> bool:
        """Update a product's existing process
        
        Args;
            process_id (str): The _id of the process to update
            process_update (dict): A mapping of the update info
        
        Returns:
            A boolean indicating if the update was successful
        
        Raises:
            Exception: see `_perform_collection_udpate`
        """
        import random
        if "name" in process_update:
            process_update["process"] = process_update.pop("name")
        allowed_fields = {
            "process", 
            "activity_value",
            "activity_unit",
            "activity_unit_type", 
            "activity", 
            "activity_id",
        }
        self.protect_request_fields(
            requested_fields=process_update, allowed_fields=allowed_fields
        )
        process_update["co2e"] = random.randint(0, 4)
        return self._perform_collection_update(
            "products", 
            find={"_id": ObjectId(process_id)}, 
            update={"$set": process_update},
            error_message=f"Process with id {process_id} not found"
        )
    
    def calculate_file_emissions(self, data: list[dict]) -> int:
        """Batch calculate emissions of uploaded files and insert the logs into db"""
        ghg_calculator = GHGCalculator(region=self.savior["region"] or "US")
        calculations = ghg_calculator.calculate_batches(
                data, savior_id=self.savior_id, return_replacements=True
            )
        return self.db.logs.bulk_write(calculations).inserted_count
    
    @property
    def all_product_stages(self):
        return { "sourcing", "assembly", "processing", "transport" }
    
    def create_product(self, product_data: DataFrame, product_name: str) -> ObjectId:
        """Create a product
        
        Args:
            product_data (DataFrame): A pandas DataFrame containing columns:    
                - activity: The activity.
                - value: The activity's value.
                - unit: The activity's unit.
                - stage: The process's stage.
                - process (str): Optional. Process name. Defaults to activity.
            product_name: The desired name of the product.
        
        Returns:
            The product_id of the created object.
        
        Raises:
            ResourceConflictError: When the product name has been taken.
                By the partner, that is.
            InvalidRequestDataError: When any process data is missing or invalid. 
        """
        savior_id = self.savior_id
        products_collection = self.db.products
        
        product_names = products_collection.distinct("name", {"savior_id": savior_id})
        print(product_name, product_names)
        if product_name in product_names:
            raise ResourceConflictError(
                "A product with that name has already been created"
            )
        product_data.assign(published=False, name=product_name)
        if not product_data[~product_data["stage"].isin(
            self.all_product_stages
        )].empty:
            raise InvalidRequestDataError("Invalid stage name")
        product_data = product_data.to_dict("records")
        product_id = ObjectId()
        now = datetime.now(tz=timezone.utc)
        import random 
        for doc in product_data:
            co2e = random.randint(0, 4)
            doc.update(
                {
                    "product_id": product_id, 
                    "savior_id": savior_id, 
                    "created_at": now, 
                    "co2e": co2e,
                    "process": doc.get("process") or doc["activity"], # `or` instead of indexing in second .get() arg 
                    "last_update": now,
                    "name": product_name
                }
            )
        products_collection.insert_many(product_data)
        return product_id

    def assert_product_publishable(self, product_id: ObjectId) -> bool:
        """Check if a product is publishable.
        
        Args:
            product_id (str): The product_id of the product/
        
        Returns:
            True if the product is publishable.
        
        Raises:
            MissingRequestDataError: When the product is missing any stage in
                attribute `required_stages`
        """
        product_stages = set(
            self.db.products.distinct(
                "stage", 
                {"savior_id": self.savior_id, "product_id": product_id}
            )
        )
        self.protect_and_require_fields(
            product_stages, 
            self.all_product_stages, 
            invalid_fields_error_prefix="Invalid product stages",
            missing_fields_error_prefix="Missing required stages"
        )
    
    def publish_product(self, product_id: str) -> bool:
        """Publish a product.
        
        Args:
            product_id (str): The product_id of the product being published.
        
        Returns:
            A boolean indicating the success of the publishing.
        
        Raises:
            MissingRequestDataError: When the product is missing stages 
                which are required. 
        """
        products_collection = self.db.products
        product_id = ObjectId(product_id)
        savior_id = self.savior_id
        now = datetime.now(tz=timezone.utc)
        self.assert_product_publishable(product_id=product_id)
        res = self._perform_collection_update(
            "products", 
            find={"savior_id": savior_id, "product_id": product_id}, 
            update={"$set": {"last_update": datetime.now(tz=timezone.utc), "published": True}},
            error_message=f"Product with id {product_id} does not exist"
        )
        products_collection.aggregate(
            [
                {"$match": {"savior_id": savior_id, "product_id": product_id}},
                {
                    "$group": {
                        "_id": None,
                        "co2e": {"$sum": "$co2e"},
                        "activity": {"$first": "$name"},
                        "name": {"$first": "$name"},
                    },
                },
                {
                    "$project": {
                        "_id": 0,
                        "co2e": 1,
                        "activity": 1,
                        "name": 1,
                        "source": "partners",
                        "image": None,
                        "rating": None,
                        "last_update": now,
                        "created_at": now,
                        "savior_id": savior_id,
                        "product_id": product_id
                    }
                },
                {"$merge": {"into": "emission_factors"}}
            ]
        )
        return res
        
    def unpublish_product(self, product_id: str) -> bool:
        """Unpubish a product
        
        Args:
            product_id (str): The product_id of the product we are unpublishing
            
        Returns:
            A boolean denoting if the product was successfully unpublished
        
        Raises:
            ResourceNotFoundError: When the product_id does not exist.
                See `_perform_collection_update` method for more
        """
        product_id = ObjectId(product_id)
        db = self.db
        db.emission_factors.delete_one(
            {"savior_id": self.savior_id, "product_id": product_id}
        )
        db.product_logs.delete_many({"product_id": product_id})
        # db.stars.delete_many({"resource_id": product_id})
        return self._perform_collection_update(
            collection_name="products",
            find={"product_id": product_id},
            update={"$set": {"published": False}},
            error_message=f"Product with id {product_id} does not exist"
        )

    def update_product(self, updates: dict[str, str], product_id: str) -> bool:
        """Update a product
        
        Modify a product's name or keywords field
        
        Args:
            updates (dict): A dictionary with fields name or keywords only
            product_id (str): The product_id of the product
        
        Returns:
            A boolean indicating the update status
        
        Raises:
            ResourceNotFoundError: When the product does not exist
            ResourceConflictError: When the product name has been taken by the same partner
        """
        self.protect_request_fields(
            requested_fields=updates, allowed_fields={"name", "keywords"}
        )
        savior_id, products = self.savior_id, self.db.products
        products_created = products.distinct(
            "name", {"savior_id": savior_id}
        )
        name_updates = updates.get("name")
        if name_updates in products_created:
            raise ResourceConflictError(
                "A product with that name has already been created"
            )
        product_id = ObjectId(product_id)
        return self._perform_collection_update(
            collection_name="products",
            find={"savior_id": savior_id, "product_id": product_id}, 
            update={"$set": {k: v.strip() for (k, v) in updates.items()}}, #strip whitespace
            error_message=f"Product with id {product_id} does not exist"
        )
        
    def delete_product(self, product_id: str) -> bool:
        """Delete a product from the collection
        
        Args:
            product_id (str): The product's product_id
        
        Returns:
            A boolean indicating the operation's success
            
        Raises: 
            ResourceNotFoundError: When a product with `product_id` is not found.
        """
        product_id = ObjectId(product_id)
        
        self._assert_resource_exists(
            collection_name="products", 
            find={"product_id": product_id, "savior_id": self.savior_id},
            error_message=f"Product with id {product_id} does not exist",
        )
        print("HEREEEE ", )
        return bool(
            self.db.products.delete_many(
                {"product_id": product_id, "savior_id": self.savior_id}
            ).deleted_count
        )
        
    def get_tasks(self, query_params: dict={}) -> list:
        """Get tasks
        
        Args:
            query_params (dict): The request.args in dictionary form.
        
        Returns:
            An empty list or list of task documents, sorted
            by creation data and completion status.
        
        Raises:
            InvalidRequestDataError: When a query param is invalid.
        """
        self.protect_request_fields(
            query_params, 
            allowed_fields={ "complete", "assignee", "type" },
            error_prefix="Can't query upon that field"
        )
        return list(
            self.db.tasks.find(
                {"savior_id": self.savior_id, **query_params}, 
                sort=[("created_at", 1), ("complete", 1)]
            )
        )
        
    def create_task(self, task_data: dict) -> ObjectId:
        # raise Exception("Invalid data")
        # TODO: HAVE TO IMPLEMENT TASK INSERTION
        return self.db.tasks.insert_one(task_data).inserted_id
        
    def complete_task(self, task_id: str, create_follow_up: bool = False) -> bool:
        """Complete a task
        
        Args:
            task_id (str): The _id of the task to complete
            
        Returns:
            A boolean indicating if the completion was successful
        
        Raises:
            Exception: If no task was found when updating
        """
        return self._perform_collection_update(
            collection_name="tasks",
            find={"_id": ObjectId(task_id)},
            update={"$set": {"complete": True}},
            error_message=f"Task {task_id} does not exist"
        )
        
    def assign_task(self, task_id: str, assignee: str) -> bool:
        """Assign a task to a user
        
        Args:
            task_id (str): The _id of the task.
            assignee (str): A valid assignee
        
        Returns:
            A boolean indicating the status of the assigning
        
        Raises:
            ResourceNotFoundError: When the assignee does not exist as a company user,
                or when the task with `task_id` is not found.
        """
        savior_id = self.savior_id
        possible_assingees = self.db.partners.distinct("username", {"savior_id": savior_id})
        if assignee not in possible_assingees:
            # raise ResourceNotFoundError(f"No user with the username {assignee} found")
            pass
        return self._perform_collection_update(
            collection_name="tasks",
            find={"savior_id": savior_id, "_id": ObjectId(task_id)},
            update={"$set": {"assignee": assignee}},
            error_message=f"Task with id {task_id} does not exist"
        )
        
        
    def process_file_logs(
        self, 
        file_logs: list[dict], 
        task_id: str | None = None,
        create_follow_up_task: bool = False,
    ) -> ObjectId:
        """Process an uploaded file's logs

        Calculate emissions, and add fields like source_file for an uploaded file.
        
        Args:
            file_logs (list[dict]): The logs to calculate emissions of,  update, 
                and insert to logs collection. Each log must be a dictionary containing 
                all and only fields: activity, value, unit, unit_type
            task_id (str): Optional. The _id of the task that initiated this file upload.
            create_follow_up_task (bool): Whether or not to create a follow up 
                with respect to the originating task. Only relevant when task_id 
                is present. Defaults to False.
        Returns:
            The source_file.id field (file id) created by the inserts
        
        Raises:
            MissingRequestDataError: When the request is missing any data.
            InvalidRequestDataError: When the request contains invalid data.
        """
        import random 
        savior_id = self.savior_id
        db = self.db
        file_id = ObjectId()
        now = datetime.now(tz=timezone.utc)
        for log in file_logs:
            log.update(
                {
                    "co2e": random.randint(0, 10), 
                    "savior_id": savior_id, 
                }
            )
            log["source_file"].update({"id": file_id, "upload_date": now})
        db.logs.insert_many(file_logs)
        if task_id:
            self.complete_task(
                task_id=task_id,
                create_follow_up=create_follow_up_task
            )
        return file_id
    
    def handle_emissions_file(
        self, file_df: DataFrame, get_form_field: ImmutableMultiDict.get, filename: str
    ) -> ObjectId:
        """Perform a file upload 
        
        Insert file logs to 'logs' collection. 
                
        Args:
            file_df: The uploaded file as a pandas DataFrame
            get_form_field (ImmutableMultiDict.get): The `get` method of the requests form
            filename: What to name the file when inserting as logs to mongodb
        
        Returns:
            the id of the file created during the upload process
        
        Raises:
            MissingRequestDataError: When the request is missing data fields
        """
        form_postable_fields = ("scope", "category", "unit_type")
        missing_columns, assigns = [], {}
        for required_col in ("activity", "value", "unit", *form_postable_fields):
            if not required_col in file_df:
                if required_col in form_postable_fields:
                    fallback = get_form_field(required_col)
                    if fallback:
                        assigns[required_col] = fallback
                    else:
                        missing_columns.append(required_col)
                else:
                    missing_columns.append(required_col)
        if len(missing_columns) > 0:
            raise MissingRequestDataError(
                f"Missing data fields: {', '.join(missing_columns)}"
            )
        elif assigns:
            file_df = file_df.assign(
                **assigns,
                ghg_category=get_form_field("ghg_category", None)
            )
        file_df.loc[:, "source_file"] = [{"name": filename}] * len(file_df)
        documents = file_df.replace({np.nan: None}).to_dict("records")
        file_id = self.process_file_logs(
            file_logs=documents, task_id=get_form_field("task_id")
        )
        return file_id

    @staticmethod
    def get_partner(db: Database, partner_id: str) -> dict:
        """Get data and products of a partner
        
        Args:
            partner_id (str): The company_id of the partner
        
        Returns:
            A dict with the insensitive account info of the partner
            and their published products
        
        Raises: 
            ResourceNotFoundError: When a partner with `partner_id` does not exist.
        """
        partner_id = ObjectId(partner_id)
        partner_card = db.partners.find_one(
            {"company_id": partner_id},
            {"name": "$company", "company": 1, "joined": 1, "region": 1, "bio": 1}
        )
        if not partner_card:
            raise ResourceNotFoundError(f"Partner with id {partner_id} not found")
        partner_card["products"] = list(
            db.emission_factors.find(
                {"savior_id": partner_id}, 
                {"name": 1, "co2e": 1, "_id": "$product_id"}
            ).sort([("created_at", -1)])
        )
        return partner_card
        

def get_upload_task(
    data_type: str, 
    scope: str, 
    category: str | None = None, 
    **kwargs
) -> dict:
   return  {
            'task': f'Upload {data_type} data',
            'complete': False,
            'category': category or data_type,
            'assignee': None,
            'scope': scope,
            "type": "collection",
            'action': 'Upload',
            **kwargs
        }

def get_scope_three_task(category_name: str, ghg_category: str) -> dict: 
    return get_upload_task(data_type=category_name, ghg_category=ghg_category, scope="3")

ghg_categories = [
    ("Scope 1", "1"),
    ("Scope 2", "2"),
    ("purchased goods and services", "3.1"),
    ("capital goods", "3.2"),
    ("fuel and energy related activities", "3.3"),
    ("upstream transportation and distribution", "3.4"),
    ("waste generated in operations", "3.5"),
    ("business travel", "3.6"),
    ("employee commuting", "3.7"),
    ("upstream leased assets", "3.8"),
    ("downstream transportation and distribution", "3.9"),
    ("processing of sold products", "3.10"),
    ("use of sold products", "3.11"),
    ("end-of-life treatment of sold products", "3.12"),
    ("downstream leased assets", "3.13"),
    ("franchises", "3.14")
]
    
GHG_CATEGORIES_TO_UPLOAD_TASKS = {
    x[1]: get_scope_three_task(*x)
    for x in ghg_categories
}