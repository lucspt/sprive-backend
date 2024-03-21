from datetime import datetime, timezone
import pymongo
from typing import Literal
from numbers import Number
from bson import ObjectId
# from openai import OpenAI

class Savior:
    __slots__ = (
        "savior_id", 
        "db",
        "savior_type",
        "client",
        "_close"
    )
    
    def __init__(self, savior_id: str, savior_type: str):
        client = pymongo.MongoClient()
        self._close = lambda: client.close()
        db, savior_id = client.spt, ObjectId(savior_id)
        self.db = db
        self.verify_savior_id(savior_id=savior_id, savior_type=savior_type)
        self.savior_type = savior_type
    
    def verify_savior_id(self, savior_id: ObjectId, savior_type: str) -> None:
        print(savior_id)
        if not self.db[savior_type].find_one({"_id": savior_id}):
            raise ValueError("No such savior")
        self.savior_id = savior_id
        return True
    
    def _get_insert(self, document: dict) -> dict:
        return {
            **document,
            "created": datetime.now(tz=timezone.utc),
            "savior_id": self.savior_id,
        }
        
    @staticmethod
    def string_to_date(date_string: str) -> datetime:
        return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z")
        
        
    Collection = pymongo.collection.Collection 
    def get_co2e(self, collection: Collection, group: str = None,) -> Number:
        """Get a sum of co2e from a savior_id and collection"""
        match = {"savior_id": self.savior_id}
        if collection.name == "logs":
            match.update({"co2e": {"$exists": True}})
        group = f"${group}" if group else group
        pipeline = [
            {"$match": match},
            {"$group": {"_id": group, "co2e": {"$sum": "$co2e"}}}
        ]
        if not group:
            result = collection.aggregate(pipeline)
            result = round(result.next()["co2e"], 2) if result.alive else 0
        else:
            result = list(
                collection.aggregate(pipeline + [
                    {"$sort": {"co2e": -1}}, {"$limit": 5}
                ])
            )
        print(pipeline)
        return result
    
    @property
    def pledges(self):
        return list(self.db.pledges.find({"savior_id": self.savior_id}))
    
    def make_pledge(self, pledge_document) -> tuple[ObjectId, bool]:
        unit_type = pledge_document["unit_type"]
        unit, value =  pledge_document["unit"], pledge_document["value"]
        # calculation = self.calculate(
        #     activity_id=pledge_document["activity"],
        #     value=value,
        #     unit=unit,
        #     unit_type=unit_type
        # )
        calculation = {"co2e": 10}
        now = datetime.now(tz=timezone.utc)
        pledge_document["name"] = pledge_document.pop("name").lower()
        co2e = calculation["co2e"]
        if pledge_document["recurring"]:
            pledge_document["status"] = "active"
        pledge = {
            **pledge_document,
            "last_updated": now,
            "created": now,
            "co2e": co2e,
            "co2e_factor": co2e,
            "stars": [],
            "savior_id": self.savior_id,
        }
        return self.db.pledges.insert_one(pledge).inserted_id, pledge["recurring"]
    
    def get_data(
        self, 
        query_type: Literal["aggregate", "find"],
        collection: str,
        filters: dict | list = {},
    ) -> list :
        _collection = self.db[collection]
        required_filters = {"savior_id": self.savior_id}
        if query_type == "find":
            if collection == "logs":
                required_filters.update(
                    {"co2e": {"$exists": True, **filters.get("co2e", {})}}
                )
                print("FIND UPDATED", required_filters)
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
                if "created" in match:
                    date_range = match["created"]
                    for accumulator, date in entrypoint["$match"]["created"].items():
                        date_range[accumulator] = self.string_to_date(date)
                        print(date_range)
                        match["created"] = date_range
            else:
                filters = [{"$match": required_filters}] + filters
            print(filters, "FILTERS")
            res = list(_collection.aggregate(filters))
            return res[0] if len(res) == 1 and query_type == "find" else res
        else: 
            raise ValueError("query_type must be one of aggregate or find")
        
    def handle_emission_factor(
        self,
        factor_id: str,
        accumulator: str
    ) -> str:
        """Add or save an emission factor"""
        update = {accumulator: {"saved_by": self.savior_id}}
        self.db.emission_factors.update_one(
            {"_id": ObjectId(factor_id)}, update, upsert=True
        )
        return factor_id
        
    def create_factor(self, factor_document: dict) -> bool:
        # embeddings = self.client.embeddings.create(
        #     model="text-embedding-3-large",
        #     input=factor_document["keywords"]
        # ).text TODO: embeddings for custom emission factors and products
        # embeddings = self.client.embeddings.create(
        #     input=factor_document["keywords"],
        #     model="text-embedding-3-small"
        # )
        savior_id = self.savior_id
        factor_document["unit_types"] = factor_document["unit_types"]
        insert = {
            **factor_document,
            # "embeddings": embeddings.data[0].embedding,
            "embeddings": [1] * 1536,
            "savior_id": savior_id,
            "source": self.savior_type,
            "saved_by": [savior_id],
            "last_updated": datetime.now(tz=timezone.utc),
            "created": datetime.now(tz=timezone.utc)
        }
        return self.db.emission_factors.insert_one(insert).acknowledged
    
    def handle_stars(
        self, 
        id: str, 
        product_info: dict | None = None,
        delete: bool = False
    ) -> bool:
        db = self.db
        product_id = ObjectId(id)
        if delete:
            # db.products.update_many(
            #     {"product_id": product_id}, {"$inc": {"stars": -1}}
            # )
            db.stars.delete_one({"savior_id": self.savior_id, "resource_id": product_id,})
        else:
            savior_id = self.savior_id
            # db.products.update_many(
            #     {"product_id": product_id}, {"$inc": {"stars": 1}}
            # )
            return db.stars.update_one(
                {"savior_id": savior_id, "resource_id": product_id}, 
                {"$set": 
                    { 
                        **product_info, 
                        "resource_id": product_id,
                        "savior_id": savior_id, 
                        "created": datetime.now(tz=timezone.utc)
                    }
                },
                upsert=True,
            ).acknowledged
    
    def get_thread(self, thread_id: str) -> list:
        return self.db.threads.find(
            {"savior_id": self.savior_id, "thread": thread_id},
            {"_id": 0, "role": 1, "content": 1},
        ).sort("created", 1)
