from datetime import datetime, timedelta, timezone
from numbers import Number
from typing import Literal
from root.saviors.savior import Savior
from bson import ObjectId

class User(Savior):
    def __init__(self, savior_id: str):
        super().__init__(savior_id=savior_id, savior_type="users")
        
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
        
    def logs(self, limit=0, skip=0) -> dict[str, list | bool]:
        """Get the product views of a user
        
        Args:
            limit: how many results to return
            skip: how many logs to skip as it is sorted by date
        
        This also returns whether the user has more logs after skip + limit amount
        """
        res = list(
            self.db.product_logs.find({"savior_id": self.savior_id})
            .sort([("created", -1)])
            .skip(skip)
            .limit(limit + 1 if limit != 0 else 0)
        )
        if limit:
            res, has_more = res[:limit], bool(res[limit:])
        else:
            has_more = False
        return {"history": res, "has_more": has_more}
        
    def log_product_emissions(self, product_id: str, value: Number = 1) -> ObjectId:
        """Calculate the emissions of a product, 
        multiplied by a value (quantity of product)"""
        product_id = ObjectId(product_id)
        self.db.emission_factors.aggregate(
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
                {"$merge": {"into": "product_logs"}}
            ]
        )
        return product_id

    def product_logs_by_day(self, tz: str, limit=0) -> list:
        """The user's product logs / emissions from products
        grouped by day, month, year and co2e for those periods"""
        
        tz = {"timezone": tz} if tz else {}
        project_logs = {"logs": {"$slice": ["$logs", -limit]}} if limit else {"logs": 1}
        pipeline = [
            {"$match": {"savior_id": self.savior_id}},
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$created",
                            **tz,
                        },
                    },
                    "co2e": {"$sum": "$co2e"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "co2e": 1,
                    "date": "$_id",
                }
            },
            {"$sort": {"date": 1}},
            {
                "$group": {
                    "_id": None,
                    "logs": {
                    "$push": "$$ROOT"
                    },
                    "average_co2e": {"$avg": "$co2e"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                   **project_logs,
                    "average_co2e": 1,
                }
            }
        ]
        return list(self.db.product_logs.aggregate(pipeline))
    
    def emissions(self, date_ranges: dict[str, str]) -> dict:
        """This function allows the front end to query for co2e amounts since certain dates 
        This is necessary because pymongo only works with datetime objects and not JSON date strings
        
        Args:
            date_ranges: A list of dicts specifying desired keys and dates to aggregate upon 
            e.g {"day": "2023-02-12"} will return everything after that date.
        """
        addFields = {}
        string_to_date = self.string_to_date
        for period, date_string in date_ranges.items():
            datetime_object = string_to_date(date_string)
            print(datetime_object)
            addFields[period] = {
                "$cond": [{"$gt": ["$log.created", datetime_object]}, "$log.co2e", 0]
            }
        res = self.db.product_logs.aggregate(
                [
                    {
                        "$group": {
                            "_id": None,
                            "total": {"$sum": "$co2e"},
                            "log": {"$push": "$$ROOT"},
                        }
                    },
                    {"$unwind": "$log"},
                    {"$addFields": addFields},
                    {
                        "$group": {
                            "_id": None,
                            "total": {"$first": "$total"},
                            **{period: {"$sum": f"${period}"} for period in addFields.keys()}
                        },
                    }, 
                    {"$project": {"_id": 0}}  
                ]
            )
        return res.next() if res.alive else {}
        
    def update_profile(self, updates: dict) -> bool:
        return self.db.users.update_one(
            {"_id": self.savior_id}, {"$set": updates}
        ).acknowledged
        
    def starred_products(self, limit: int = 0, skip: int = 0):
            res = list(
                self.db.stars
                .find({"savior_id": self.savior_id})
                .sort([("created", -1)])
                .skip(skip)
                .limit(limit + 1)
            )
            if limit:
                res, has_more = res[:limit], bool(res[limit:])
            else: 
                res, has_more = res, False
            return {"starred": res, "has_more": has_more}
            
    def make_pledge(self, pledge_document) -> bool:
        return self.db.users.update_one(
            {"_id": self.savior_id}, {"$set": {"current_pledge": pledge_document}}
        ).acknowledged
        
    def get_times_logged(self, since_date: str) -> bool:
        """Returns the counthow many logs a user made
        since a given date
        
        Args:
            since_date: The date to start the query from when counting logs
        """
        return self.db.product_logs.count_documents(
            {
             "savior_id": self.savior_id,
             "created": {"$gt": self.string_to_date(since_date)}
            }
        )
        
    def start_spriving(self) -> bool:
        return self.db.users.update_one(
            {"_id": self.savior_id}, {"$set": {"spriving": True}}
        ).acknowledged
    
    def stop_spriving(self) -> bool:
        return self.db.users.update_one(
            {"_id": self.savior_id}, {"$set": {"spriving": False}}
        )
        
    # @property
    # def overview(self) -> dict:
    #     """Get an overview for the user
        
    #     Returns:
    #         - the co2e of the past week for the user
    #         - the total co2e of the user
    #         - number of products a user has starred
            
    #     this is what populates the bottom stats bar in the front end"""
    #     db = self.db
    #     savior_id = self.savior_id
    #     pledge_frequency = db.users.find_one(
    #         {"_id": savior_id}, {"current_pledge": 1, "_id": 0}
    #     ).get("frequency", "week")
    #     now = datetime.now()
        
    #     if pledge_frequency == "week":
    #         date_query = (
    #             now - timedelta(
    #             days=now.weekday(), 
    #             seconds=now.second, 
    #             minutes=now.minute, 
    #             microseconds=now.microsecond, 
    #             hours=now.hour
    #             )
    #         )
    #     elif pledge_frequency == "day":
    #         date_query = datetime(
    #             year=now.year, 
    #             month=now.month, 
    #             day=now.day,
    #             hour=0, 
    #             second=0,
    #             microsecond=0
    #         )
    #     elif pledge_frequency == "month":
    #         date_query = datetime(
    #             year=now.year, month=now.month, day=1, hour=0, second=0, microsecond=0
    #         )
    #     elif pledge_frequency == "year":
    #         date_query = datetime(
    #             now.year, month=1, day=1, hour=0, second=0, microsecond=0
    #         )
    #     emissions = db.product_logs.aggregate(
    #         [
    #             {"$match": {"savior_id": savior_id}},
    #             {
    #                 "$group": {
    #                     "_id": None,
    #                     "total_co2e": {"$sum": "$co2e"},
    #                     "log": {"$push": "$$ROOT"},
    #                 }
    #             },
    #             {"$unwind": "$log"},
    #             {
    #                 "$addFields": {
    #                     "periodic_co2e": {
    #                         "$cond": [
    #                             {
    #                                 "$gt": [
    #                                     "$log.created", date_query.astimezone(timezone.utc)]
    #                             }, "$log.co2e", 0
    #                         ]
    #                     }
    #                 },
    #             },
    #             {
    #                 "$group": {
    #                     "_id": None,
    #                     "periodic_co2e": {"$sum": "$periodic_co2e"},
    #                     "total_co2e": {"$first": "$total_co2e"}
    #                 },
    #             },
    #         ]
    #     )
    #     emissions = (
    #         emissions.next() if emissions.alive
    #         else {"total_co2e": 0, "periodic_co2e": 0}
    #     )
    #     return {
    #         **emissions,
    #         "num_stars": db.stars.count_documents({"savior_id": self.savior_id})
    #     }
         
    # def logs(self, limit=0, skip=0):
    #     return list(
    #         self.db.product_logs.find({"savior_id": self.savior_id})
    #         .sort([("created", -1)])
    #         .skip(skip)
    #         .limit(limit)
    #     )
        