"""User class to handle incoming requests from users"""

from numbers import Number
from root.savior import Savior
from bson import ObjectId
from pymongo.cursor import Cursor
from datetime import datetime, timezone
from typing import override
from exceptions import ResourceNotFoundError

class User(Savior):
    """Handle CRUD operations requested by a user.
    
    Attributes:
        savior (dict): The requesting savior's account.
            No sensitive information is returned from this 
            attribute.
    """
    
    @override
    def __init__(self, savior_id: str):
        """Initialize a savior class for a user.
        
        See `Savior` initializer for more.
        """
        super().__init__(savior_id=savior_id)
        
    @property
    def savior(self):
        return self.db.users.find_one(
            {"_id": self.savior_id}, 
            {
                "email": 1, 
                "current_pledge": 1, 
                "username": 1, 
                "savior_id": "$_id", 
                "spriving": 1,
                "_id": 0,
             },
        )
        
    @classmethod
    def skip_limit_cursor(
        self, cursor: Cursor, limit: int, skip: int
    ) -> tuple[list, bool]:
        """Helper function for paginating when requesting collection inserts
        
        Note: It's likely desired to first sort `cursor` by date,
        since we are paginating it.
        
        Args:
            cursor (pymongo.Cursor): The cursor instance to paginate.
            limit (int): how many results to return from the cursor result.
            skip (int): how many results to skip.
        
        Returns:
            A tuple with the page results and a boolean indicating
            if there are more pages to query
        """
        res = list(
            cursor
            .skip(skip)
            .limit(limit + 1 if limit != 0 else limit)
        )
        if limit:
            res, has_more = res[:limit], bool(res[limit:])
        else: 
            res, has_more = res, False
        return res, has_more
        
    def logs(self, limit=0, skip=0) -> dict[str, list | bool]:
        """The user's logs, sorted by date"""
        res, has_more = self.skip_limit_cursor(
            self.db.product_logs.find(
                {"savior_id": self.savior_id}
            ).sort([("created_at", -1)]),
            skip=skip,
            limit=limit
        )
        return {"logs": res, "has_more": has_more}
        
    def log_product_emissions(self, product_id: str, value: int = 1) -> dict:
        """Log emissions for a product.
        
        Given a value, multiply a product's co2e emissions to calculate
        the product's impact.
        
        Args:
            product_id (str): The product_id of the product being logged.
            value (int): Optional. The value to multiply product emissions 
                by when logging. Defaults to 1
        
        Returns:
            A dict of the resulting log
            
        Raises:
            Exception: When no product with `product_id` is found.
        """
        product_id = ObjectId(product_id)
        db = self.db
        res = db.emission_factors.aggregate(
            [
                {"$match": {"source": "partners", "product_id": product_id}},
                {
                    "$project": {
                        "co2e": {"$multiply": ["$co2e", value]},
                        "value": value,
                        "unit": "count",
                        "unit_type": "count",
                        "name": 1,
                        "product_id": product_id,
                        "_id": 0,
                        "image": 1,
                        "rating": 1,
                        **self._get_insert({})
                    }
                },
            ]
        )
        if res.alive:
            res = res.next()
        else:
            raise ResourceNotFoundError(
                f"A product with the id {product_id} does not exist"
            )
        db.product_logs.insert_one(res)
        return res 
        
    
    def update_profile(self, updates: dict) -> bool: 
        """Update a user's profile
        
        Args:
            updates (dict): A dictionary containing one or more of the following fields:
                - username
                - name
                - password
                - email
                
        Returns:
            A boolean indicating if an update took place
        
        Raises:
            Exception: When one or more fields of `updates` are invalid
                or not allowed
        """
        self.protect_request_fields(
            updates, {"username", "name","password", "email"}
        )
        return bool(
            self.db.users.update_one(
                {"_id": self.savior_id}, {"$set": updates}
            ).modified_count
        )
        
    def handle_stars(self, product_id: str, delete: bool) -> bool:
        # kept as a single function to reduce repitition
        """Star and Unstar a product.
        
        Args:
            product_id (str): The product_id of the product being unstarred.
            delete (bool): If true, unstar the product. Otherwise, star it.
            
        Returns:
            A boolean indicating if the operation took place.
            
        Raises:
            Exception: When no product with `product_id` exists
        """
        db, product_id = self.db, ObjectId(product_id)
        product_info = self._assert_resource_exists(
            collection_name="emission_factors",
            find={"product_id": product_id},
            error_message=f"A product with id {product_id} does not exist",
            projection={"_id": 0, "name": 1, "co2e": 1, "image": 1}
        )[0]
        if not product_info:
            raise ResourceNotFoundError(
                f"A product with id {product_id} does not exist"
            )
        else:
            savior_id = self.savior_id
            if delete:
                res = db.stars.delete_one(
                        {"savior_id": self.savior_id, "resource_id": product_id}
                    ).deleted_count
            else:
                res = db.stars.update_one(
                    {"savior_id": savior_id, "resource_id": product_id}, 
                    {"$set": 
                        { 
                            **product_info,
                            "resource_id": product_id,
                            "savior_id": savior_id, 
                            "created_at": datetime.now(tz=timezone.utc)
                        }
                    },
                    upsert=True,
                )
                res = res.upserted_id or res.modified_count
            return bool(res)
        
    def starred_products(self, limit: int = 0, skip: int = 0) -> dict[str, bool | list]:
        """Get starred products, sorted by descending date"""
        res, has_more = self.skip_limit_cursor(
            self.db.stars.find({"savior_id": self.savior_id})
            .sort([("created_at", -1)]),
            limit=limit,
            skip=skip
        )
        return {"starred": res, "has_more": has_more}
            
    def pledge(self, pledge_document: dict[str, str | Number]) -> bool:
        """Create or update the current pledge.
        
        Args:
            pledge_document (dict): A dictionary containing fields:
                frequency (Literal["day", "week", "month", "year"]):
                    The frequency to keep track of this pledge. This value 
                    will determine the time period they will stay within
                    their pledge co2e amount.
                co2e (Number): A Number specifying the co2e they pledge to 
                    stay within during frequency period.
                message (str): Optional. A message the user would like to the pledge with.
        
        Returns:
            A boolean indicating if the update took place.
        
        Raises:
            Exception: If frequnecy or co2e is not present in the request, or if fields other 
                than ones specified above are present.
        """
        self.protect_and_require_fields(
            pledge_document, 
            required_fields={"frequency", "co2e"},
            allowed_fields={"frequency", "co2e", "message"},
            invalid_fields_error_prefix="Can't intepret fields"
        )
        return bool(
            self.db.users.update_one(
                {"_id": self.savior_id}, {"$set": {"current_pledge": pledge_document}},
            ).modified_count
        )
        
        
    def get_times_logged(self, since_date: str) -> int: 
        """Get the amount of times a user has logged
        
        Args:
            since_date (str): The date to start from when counting logs
            
        Returns:
            An int of how many times the user has logged a product.
        """
        print("HERERERE", self.string_to_date(since_date))
        return self.db.product_logs.count_documents( 
            {
             "savior_id": self.savior_id,
             "created_at": {"$gt": self.string_to_date(since_date)}
            }
        )
        
    def start_spriving(self) -> bool: 
        """Start spriving, AKA subscribe to a membership
        
        Returns:
            A boolean indicating if the update was successful
        """
        return bool(
            self.db.users.update_one(
                {"_id": self.savior_id}, {"$set": {"spriving": True}}
            ).modified_count
        )
    
    def stop_spriving(self) -> bool:
        """Stop spriving, AKA cancel subscription
        
        Returns:
            A boolean indicating if the update was successful
        """
        return bool(
            self.db.users.update_one(
                {"_id": self.savior_id}, {"$set": {"spriving": False}}
            ).modified_count
        )
        
    def undo_pledge(self) -> bool:
        """Undo or set current pledge to None
        
        Returns:
            A boolean indicating if the update was successful"""
        return bool(
            self.db.users.update_one(
                {"savior_id": self.savior_id}, 
                {"$set": {"current_pledge": None}}
            )
        )
    
    