"""Base class for a savior.

Implements common functionalities for user and partner CRUD operations.
"""

from datetime import datetime, timezone
import pymongo
from typing import Literal, Any
from pymongo.collection import Collection
from bson import ObjectId
from typing import Type
from exceptions import (
    ResourceNotFoundError, MissingRequestDataError, InvalidRequestDataError
)

class Savior:
    """Create a `Savior` instance.
    
    Mostly used as a base class for inheriting common 
    savior operations, though there are also some use cases
    and static methods for direct usage.
    
    Attributes:
        _close (Callable): A lambda function to close
                the MongoClient that was created when initiating
                this class.
        db (pymongo.Database): The mongodb database holding all
            collections a partner or user will access.
        savior_id (str): The relevant _id of the savior's account.
            Will be used to query collections.
        """
    __slots__ = (
        "savior_id", 
        "db",
        "_close"
    )
    
    def __init__(self, savior_id: str):
        """Initializes a Savior instance.
        
        Args:
            savior_id (str): The account _id of the savior to initialize.
        """
        client = pymongo.MongoClient()
        self._close = lambda: client.close()
        self.db, self.savior_id = client.spt, ObjectId(savior_id)
    
    def _get_insert(self, document: dict={}) -> dict:
        """Prepare a document for a collection insert.
        
        Args:
            document (dict): The document to create an insertable from.
        
        Returns:
            The dict passed to document with fields created and savior_id added.
            Created being the current datetime in UTC format
        """
        return {
            **document,
            "created_at": datetime.now(tz=timezone.utc),
            "savior_id": self.savior_id,
        }
        
    @staticmethod
    def string_to_date(date_string: str) -> datetime:
        """Turn an ISO8601 string to a datetime object.
        
        Pymongo interacts with datetime objects for dates.
        This helper method will turn an ISO8601 string,
        the expected format from requests, into a 
        datetime object for collection querying.
        
        Args:
            date_string (str): The ISO date string
        
        Returns:
            A datetime object.
        """
        return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z")    

    @staticmethod
    def protect_request_fields(
        requested_fields: dict | set,
        allowed_fields: set,
        error_prefix: str = "Can't modify fields",
        ) -> bool:
        """Make sure a request's dict keys only include `allowed_fields`.
        
        See `protect_and_require_fields` for more.
        """
        if isinstance(requested_fields, dict): 
            requested_fields = requested_fields.keys()
        not_allowed = requested_fields - allowed_fields
        if not_allowed:
            raise InvalidRequestDataError(
                f"{error_prefix}: {", ".join(not_allowed)}"
            )
        return True
    
    @staticmethod
    def require_request_fields(
        requested_fields: dict | set,
        required_fields: set,
        error_prefix: str = "Missing required fields",
    ) -> bool:
        """Make sure a request's dict keys include `required_fields`.
        
        See `protect_and_request_fields` for more.
        """
        if isinstance(requested_fields, dict):
            requested_fields = requested_fields.keys()
        missing_fields = required_fields - requested_fields
        if missing_fields:
            raise MissingRequestDataError(
                f"{error_prefix}: {", ".join(missing_fields)}"
            )
        return True
    
    @classmethod
    def protect_and_require_fields(
        self,
        requested_fields: dict | set,
        fields: set | None = None,
        allowed_fields: set | None = None,
        required_fields: set | None = None,
        invalid_fields_error_prefix: str = "Can't modify that field",
        missing_fields_error_prefix: str = "Missing required fields",
    ) -> bool:
        """Allow and require certain fields to be in a request
        
        When a savior makes a request that can only contain certain keys,
        or must contain certain keys, This helper method will make sure
        the request doesn't violate the specified rules. Either `fields` 
        or `required_fields` and `allowed_fields` need to be present 
        when calling this function.
        
        Args:
        requested_fields (dict | set): The request dictionary 
            or a set of the keys / fields being requested.
        fields (set): Optional. 
        allowed_fields (set): The fields that are 
            allowed to be requested.
        missing_fields_error_prefix (str): Optional. What to prefix the error with,
            when one is raised because a request contains violates request expectations.
        missing_fields_error_prefix (str): Optional. What to prefix the error with,
            when one is raised because a request is missing data.
        
            
        Returns:
            `True` if the request is valid.
        
        Raises:
            Exception: When the request violates the given rules of the invocation.
            ValueError: When the invocation fails to specify `fields` or .
                `required_fields` and `allowed_fields`.
        """
        if (not fields) and (not any((allowed_fields, required_fields))):
            raise ValueError(
                "Missing `fields` or `allowed_fields` and `required_fields` argument"
            )
        self.protect_request_fields(
            allowed_fields=fields or allowed_fields, 
            requested_fields=requested_fields,
            error_prefix=invalid_fields_error_prefix,
        )
        self.require_request_fields(
            required_fields=fields or required_fields, 
            requested_fields=requested_fields,
            error_prefix=missing_fields_error_prefix
        )
        return True
    
    def _assert_resource_exists(
        self,
        collection_name: str,
        find: dict[str, ObjectId | Any],
        error_message: str,
        projection: set | dict | None = None,
    ) -> list:
        """Helper that makes sure a resource exists in a collection.
        
        If the resource is non existant from `collection_name` given `find`
        raise a ResourceNotFoundError.
        
        Note: if a resource is found, a list will be returned even if just one result
        is present.
        
        Args:
            collection_name (str): The collection to query.
            find (dict): A dictionary query mapping to pass to pymongo.Collection.find().
            error_message (str): The error message to pass to the error when resource is not found.
            projection (set | dict | None): Optional. A projection to pass to pymongo.Collection.find()
        
        Returns:
            A list containing the resource(s), if found.
            
        Raises:
            ResourceNotFoundError: When no resources are found.
        """
        print("HEREE")
        resource = list(self.db[collection_name].find(find, projection))
        print("resousrce here", resource, find)
        if not resource:
            raise ResourceNotFoundError(error_message)
        return resource
    
    def _perform_collection_update(
       self, 
       collection_name: str, 
       find: dict[str, ObjectId | Any], 
       update: dict,
       error_message: str, 
    ) -> bool:
        """Helper method for updating collection documents.
        
        This function will perform a `pymongo.Collection.update_many()`
        with `find` as its filter, if the requested resource exists.
            
        Args:
            collection_name (str): The name of the collection containing the document.
            find (dict): A dictionary to find the resource being updated.
            update (dict): The updates to perform on the document.
            error_message (str): The error message when raising an error 
                if `find` returns None
        Returns:
            A boolean indicating whether the update was sucessful
        
        Raises:
            Exception: When the find argument fails to find any document.
                See `_assert_resource_exists` for more.
        """
        self._assert_resource_exists(
            collection_name=collection_name,
            find=find,
            error_message=error_message
        )
        return bool(self.db[collection_name].update_many(find, update).modified_count)
        
    def get_data(
        self, 
        query_type: Literal["aggregate", "find"],
        collection: str,
        filters: dict | list = {},
    ) -> list:
        """Perform an aggregate or find method on a `pymongo.Collection`.
        
        Given the filters collection name and query_type, craft an
        aggregation or find query to call on a pymongo collection.
        The requesting savior can only access their own data.
        Dates in $match stages when query_type is 'aggregation' 
        are parsed and transformed to `datetime` objects.
        
        Args:
            query_type (Literal["aggregate", "find"]): The type of method 
                to perform on the collection.
            collection (str): The name of the collection to query.
            filters (list | dict): A pipeline list to call on the collection
                if aggregating, or a filters dictionary to find from the collection.
                
        Returns:
            The result of the find or aggregation  
        
        Raises:
            InvalidRequestDataError: When `query_type` is not equal to aggregate or find.
        """
        _collection = self.db[collection]
        required_filters = {"savior_id": self.savior_id}
        if query_type == "find":
            if collection == "logs":
                required_filters.update(
                    {"co2e": {"$exists": True, **filters.get("co2e", {})}}
                )
            filters.update(required_filters)
            return list(_collection.find(
                {"savior_id": self.savior_id, **filters},
            ))
        elif query_type == "aggregate":
            entrypoint = filters[0]
            if "$match" in entrypoint:
                match = entrypoint["$match"]
                if collection == "logs":
                    required_filters.update(
                        {"co2e": {"$exists": True, **match.get("co2e", {})}}
                    )
                match.update(required_filters)
                if "source_file.upload_date" in match:
                    date_range = match["source_file.upload_date"]
                    for accumulator, date in entrypoint["$match"]["source_file.upload_date"].items():
                        date_range[accumulator] = self.string_to_date(date)
                        match["source_file.upload_date"] = date_range
            else:
                filters = [{"$match": required_filters}] + filters
            res = list(_collection.aggregate(filters))
            return res[0] if len(res) == 1 and query_type == "find" else res
        else: 
            raise InvalidRequestDataError("query_type must be one of aggregate or find")
        
    @staticmethod
    def collection_text_search(
        collection: Collection, 
        query_params: dict, 
        matches: dict = {},
        projections: dict = {},
        result_dict_field: str = "results",
    ) -> dict[str, list | bool]:
        """Fulfill a text search request on a collection.
        
        Args:
            collection (Collection): The collection to perform text search upon.
            query_params (dict): A dictionary containing the query params from 
                the request.
            matches (dict): Any matches to add to the aggregation pipeline's
                initial $match stage
            projections (dict): Any projections to add to the aggregation pipeline's
                final $project stage
            result_dict_field (str): What to name the key of the return
        
        Returns:
            A dict with keys has_more and `result_dict_field` argument.
            They are boolean and list values, respectively 
        """
        sort = {"last_update": -1}
        if "q" in query_params:
            query_params["$text"] = {"$search": query_params.pop("q")}
            projections.update({"relevance": {"$meta": "textScore"}})
            sort["relevance"] = -1
        limit = int(query_params.pop("limit", 0))
        skip = int(query_params.pop("skip", 0))
        matches.update(query_params)
        pipeline = [
            {"$match": matches},
            {"$project": projections}, 
            {"$sort": sort}
        ]
        if limit:
            pipeline.append({"$limit": limit + 1})
        if skip:
            pipeline.append({"$skip": skip})
        res = list(collection.aggregate(pipeline))  
        if limit:
            res, has_more = res[:limit], bool(res[limit:])
        else:
            has_more = False
        return {result_dict_field: res, "has_more": has_more}
        
    # def handle_emission_factor(
    #     self,
    #     factor_id: str,
    #     accumulator: str
    # ) -> str:
    #     """Add or save an emission factor"""
    #     update = {accumulator: {"saved_by": self.savior_id}}
    #     self.db.emission_factors.update_one(
    #         {"_id": ObjectId(factor_id)}, update, upsert=True
    #     )
    #     return factor_id
        
    # def create_factor(self, factor_document: dict) -> bool:
    #     # embeddings = self.client.embeddings.create(
    #     #     model="text-embedding-3-large",
    #     #     input=factor_document["keywords"]
    #     # ).text TODO: embeddings for custom emission factors and products
    #     # embeddings = self.client.embeddings.create(
    #     #     input=factor_document["keywords"],
    #     #     model="text-embedding-3-small"
    #     # )
    #     savior_id = self.savior_id
    #     factor_document["unit_types"] = factor_document["unit_types"]
    #     insert = {
    #         **factor_document,
    #         # "embeddings": embeddings.data[0].embedding,
    #         "embeddings": [1] * 1536,
    #         "savior_id": savior_id,
    #         "source": self.savior_type,
    #         "saved_by": [savior_id],
    #         "last_update": datetime.now(tz=timezone.utc),
    #         "created_at": datetime.now(tz=timezone.utc)
    #     }
    #     return self.db.emission_factors.insert_one(insert).acknowledged
