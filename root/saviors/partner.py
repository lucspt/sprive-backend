from root.saviors.savior import Savior
from bson import ObjectId
from typing import Literal
from datetime import datetime, timezone
from numbers import Number
from root.emissions import GHGCalculator

class Partner(Savior):
    __slots__ = ("ghg_calculator")
    
    def __init__(self, savior_id: str):
        super().__init__(savior_id=savior_id, savior_type="partners")
        self.savior_type = "partners"
        self.ghg_calculator = GHGCalculator(region=self.savior["region"] or "US")
        
    @property
    def savior(self) -> dict:
       return {
           **self.db.partners.find_one(
            {"_id": self.savior_id}
            ), 
            "savior_type": "partners"
        }
    
    def update_profile(self, updates: dict) -> bool:
        return self.db.users.update_one(
            {"_id": self.savior_id}, {"$set": updates}
        ).acknowledged
        
    def calculate(
        self,
        activity_id: str,
        activity_value: Number,
        activity_unit: str,
        activity_unit_type: str,
        activity: str | None = None
    ) -> dict:
        "emissions calculation"
        emissions = self.ghg_calculator(
            activity_id=activity_id,
            value=activity_value,
            unit=activity_unit,
            unit_type=activity_unit_type
        )
        if activity: emissions["activity"] = activity
        
        return {
            **emissions, 
            "activity_unit_type": activity_unit_type,
            "activity_unit": activity_unit,
            "activity_value": activity_value,
            "activity_id": activity_id,
            "tool_call_query": None, #this comes from amulet only
        }
        
    def insert_logs(self, logs: dict | list) -> list:
        if isinstance(logs, dict): logs = [logs]
        documents = [self._get_insert(log) for log in logs]
        inserts = self.db.logs.insert_many(documents)
        return inserts.inserted_ids
    
    @property   
    def current_stats(self): 
        savior_id = self.savior_id
        pledges = self.db.pledges 
        pledge_count = pledges.count_documents({"savior_id": savior_id})
        active_pledges = pledges.count_documents(
            {"savior_id": savior_id, "status": "active"}
        )
        emissions = self.db.logs.aggregate(
            [
                {"$match": {"savior_id": savior_id, "co2e": {"$exists": True}}},
                {
                    "$group": {
                        "_id": "$category",
                        "co2e": {"$sum": "$co2e"},
                    },
                },
                {
                    "$group": {
                        "_id": None,
                        "total_co2e": {"$sum": "$co2e"},
                        "logs": {"$push": "$$ROOT"}
                    }
                },
                {"$unwind": "$logs"},
                {
                    "$project": {
                        "percentage": {
                            "$multiply": [{"$divide": ["$logs.co2e", "$total_co2e"]}, 100]
                        },
                        "co2e": "$logs.co2e",
                        "label": "$logs._id" ,
                        "_id": 0,
                        "total_co2e": 1,
                    },
                },
                {
                    "$group": {
                        "_id": None,
                        "total_co2e": {"$first": "$total_co2e"},
                        "co2e_per_category": {
                            "$push": {
                                "percentage": "$percentage",
                                "label": "$label",
                                "co2e": "$co2e"
                            }
                        }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                    }
                }
            ]
            )
        return {  
                "emissions": emissions.next() if emissions.alive else {},
                "pledges": {
                    "active": active_pledges,
                    "count": pledge_count,
                    "total_co2e": 161.74951160064768,
                }
            }
    
    @property          
    def overview(self) -> dict:
        """Get an overview for partner dashboard"""
        savior_id = self.savior_id
        return {
            **self.current_stats, 
            "products": list(
                self.db.emission_factors.find(
                    {"savior_id": self.savior_id, "source": "partners"}
                )
            ),
            "unprocessed_files": self.db.logs.distinct(
                "source_file.name",
                {"savior_id": savior_id, "co2e": {"$exists": False}}
            )
        }
    
    def get_files(self, file: str = None, processing_only: bool = False):
        """Get the uploaded files of a partner
        
        Args:
            - file: the file_id to retrieve
            - processing_only: whether to only get files missing co2e calculations
        """
        if processing_only:
            return list(self.db.logs.find({
                "savior_id": self.savior_id,
                "source_file.name": file,
                "co2e": {"$exists": False}
            }))
        elif file:
            pipeline =  [
                {"$match": {"savior_id": self.savior_id, "source_file.name": file}},
                {
                    "$addFields": {
                        "processed": {"$gt": ["$co2e", None]}
                    }
                },
                {"$sort": {"processed": 1, "created": -1}}
            ]
        else:
            pipeline = [
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
                        "_id": "$source_file.name",
                        "needs_processing": {
                            "$addToSet": "$needs_processing"
                        },
                        "size": {"$first": "$source_file.size"},
                        "co2e": {"$sum": "$co2e"},
                        "date": {"$min": "$created"},
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
        ]
        return list(self.db.logs.aggregate(pipeline))
    
    def add_supplier(self, suppliers: str | list[str]) -> ObjectId:
        """Add a supplier to database"""
        return self.db.saviors.update_one(
            {"savior_id": self.savior_id}, 
            {"$addToSet": {"suppliers": suppliers}}, 
            upsert=True
        ).upserted_id
        
    @property
    def suppliers(self): 
        """The partners added suppliers"""
        return list(
           self.db.saviors.aggregate(
               [
                    {"$match": {"_id": self.savior_id}},
                    {"$unwind": "$suppliers"},
                    {
                        "$lookup": {
                            "from": "suppliers",
                            "localField":"suppliers._id",
                            "foreignField": "_id",
                            "as": "info"
                        }
                    },
                    {
                        "$replaceRoot": { 
                            "newRoot": {
                                "$mergeObjects": [{"$arrayElemAt": ["$info", 0]}, "$suppliers"] 
                            }
                        }
                    }
                ]
            )
        )
        
    @property
    def products(self):
        """The partner's products"""
        products = list(self.db.products.aggregate(
            [
                {"$match": {"savior_id": self.savior_id}},
                {
                    "$group": {
                        "_id": "$product_id", 
                        "co2e": {"$sum": "$co2e"},
                        "keywords": {"$first": "$keywords"},
                        "category": {"$first": "$category"},
                        "product_id": {"$first": "$product_id"},
                        "rating": {"$first": "$rating"},
                        "created": {"$first": "$created"},
                        "last_updated": {"$first": "$last_updated"},
                        "name": {"$first": "$name"}
                    }
                },
                {"$sort": {"last_updated": -1, "created": -1}}
            ]
        ))
        return products

    def get_product(self, product_id: str) -> list:
        """Get a product and all it's stages"""
        res = self.db.products.aggregate([
            {"$match": {"savior_id": self.savior_id, "product_id": ObjectId(product_id)}},
            {
                "$group": {
                    "_id": "$stage",
                    "co2e": {"$sum": "$co2e"},
                    "keywords": {"$first": "$keywords"},
                    "published": {"$first": "$published"},
                    "last_updated": {"$first": "$created"},
                    "name": {"$first": "$name"},
                    "product_id": {"$first": "$product_id"},
                    "unit_types": {"$first": "$unit_types"},
                    "activity": {"$first": "$activity"},
                    "stars": {"$first": "$stars"},
                    "processes": {"$push": {
                    "_id": "$_id",
                    "process": "$process",
                    "activity": "$activity",
                    "activity_id": "$activity_id",
                    "activity_unit": "$activity_unit",
                    "activity_unit_type": "$activity_unit_type",
                    "activity_value": "$activity_value",
                    "co2e": "$co2e"
                    }}
                }
            },
            {"$group": {
                "_id": None, 
                "stages": {
                    "$push": {
                        "co2e": {"$sum": "$processes.co2e"},
                        "num_processes": {"$size": "$processes"},
                        "stage": "$_id",
                        "processes": "$processes",
                        "last_updated": "$last_updated",
                    }
                },
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
    
    
    def delete_product_process(self, process_id: str) -> bool:
        """Delete a process from a product stage"""
        return self.db.products.delete_one({"_id": ObjectId(process_id)})
    
    @staticmethod
    def calculate_emissions() -> dict[str, int]:
        import random
        return {"co2e": random.randint(0, 5)}
    
    def create_product_process(
        self, 
        product_id: str, 
        stage: Literal["sourcing", "assembly", "processing", "transport"], 
        process_data: dict,
    ) -> bool:
        import random 
        process_data["co2e"] = random.randint(0, 4)
        now = datetime.now()
        return self.db.products.insert_one(
            {
                "product_id": ObjectId(product_id), 
                "stage": stage, 
                **process_data, 
                "savior_id": self.savior_id,
                "last_updated": now,
                "created": now,
                "name": process_data["process"],
                
            }
        ).inserted_id
        
    def update_product_process(self, process_id: str, process_update: dict):
        # process_update["co2e"] = self.calculate_emissions()
        import random
        process_update["co2e"] = random.randint(0, 4)
        return self.db.products.update_one(
            {"_id": ObjectId(process_id)}, {"$set": process_update}
        ).acknowledged
    
    
    def handle_product_processes(
        self, 
        process_update: dict, 
        id: str | None = None, 
        calculate_emissions: bool = True
    ) -> ObjectId:
        """This edits a partners product and by default 
        will calculate new emissions of the edit to write to the database
        """
        emissions_calculation = {}
        if calculate_emissions:
            print("CALCULATING")
            # emissions_calculation = self.calculate(
            #     activity_id=process_update["activity_id"],
            #     activity=process_update["activity"],
            #     activity_value=process_update["activity_value"],
            #     activity_unit=process_update["activity_unit"],
            #     activity_unit_type=process_update["activity_unit_type"]
            # )
            import random 
            emissions_calculation = {
                "co2e": random.randint(0, 20)
            }
        print("P UPDATE", process_update, "ID", id)
        process_update["published"] = False
        process_update = self._get_insert({**process_update, **emissions_calculation})
        if id:
            return self.db.products.replace_one(
                {"_id": ObjectId(id)}, process_update, upsert=True
            ).upserted_id
        else:
            process_update["stars"] = []
            return self.db.products.insert_one(process_update).inserted_id
            
    # def create_factor(self, factor_document: dict, update_product: str = "") -> dict:
    #     insert = super().create_factor(factor_document)
    #     print(update_product)
    #     if update_product:
    #         print("UPDATTTINGG", update_product)
    #         print(factor_document, "FACTOR DOC")
    #         activity = factor_document["activity"]
    #         self.products.update_many(
    #             {"product_id": update_product},
    #             {
    #                 "$set": {
    #                     "activity_id": activity, 
    #                     "name": activity, 
    #                     "keywords": factor_document["keywords"]
    #                 },
    #             }
    #         )
    #     return insert 
    
    def calculate_file_emissions(self, data: list[dict]) -> bool:
        """Batch calculate emissions of uploaded files and insert the logs into db"""
        calculations = self.ghg_calculator.calculate_batches(
                data, savior_id=self.savior_id, return_replacements=True
            )
        return self.db.logs.bulk_write(calculations).acknowledged
    
    def create_product(self, product_data: object, product_name: str) -> bool:
        """This creates a product and adds the fields needed for our denormalized structure
        
        Args:
            product_data: A pandas DataFrame containing the processes level documents of the product life cycle
            product_name: The name of the product
        """
        savior_id = self.savior_id
        products_collection = self.db.products
        product_names = products_collection.distinct("name", {"savior_id": savior_id})
        if product_name in product_names:
            raise Exception("Product names must be unique")
        product_data.assign(published=False, name=product_name)
        if not product_data[~product_data["stage"].isin(
            ["sourcing", "assembly", "transport", "processing"]
        )].empty:
            raise Exception("Invalid stage name")
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
                    "created": now, 
                    "co2e": co2e,
                    "process": doc["activity"],
                    "last_updated": now,
                    "name": product_name
                }
            )
        products_collection.insert_many(product_data)
        return product_id
    
    
    def publish_product(self, product_id: str) -> bool:
        """This publishes a product for the partner by creating an
        emission factor from it and updating the `published` (boolean) 
        field in the products collection
        
        
        We create an emission factor out of the product for the following reasons:
            - A partner might want to use it as a factor themselves
            - The amulet needs embeddings in order to retrieve a product
        """ 
        collection = self.db.products
        product_id = ObjectId(product_id)
        savior_id = self.savior_id
        now = datetime.now(tz=timezone.utc)
        collection.aggregate(
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
                        "last_updated": now,
                        "created": now,
                        "savior_id": savior_id,
                        "product_id": product_id
                    }
                },
                {"$merge": {"into": "emission_factors"}}
            ]
        )
        return collection.update_many(
            {"savior_id": savior_id, "product_id": product_id}, 
            {"$set": {"last_updated": datetime.now(tz=timezone.utc), "published": True}}
        ).acknowledged
        
    def unpublish_product(self, product_id: str) -> bool:
        """This will unpublish a product and remove all public access to it"""
        product_id = ObjectId(product_id)
        db = self.db
        db.emission_factors.delete_one(
            {"savior_id": self.savior_id, "product_id": product_id}
        )
        db.product_logs.delete_many({"product_id": product_id})
        return db.products.update_many(
            {"product_id": product_id}, {"$set": {"published": False}}
        ).acknowledged
    
    @property
    def emissions(self):
        """Returns the logged emissions of a partner"""
        return list(self.db.logs.find(
            {"savior_id": self.savior_id, "co2e": {"$exists": True}}, sort=[("created", -1)]
        ))

    def update_product(self, name_update: str, product_id: str) -> bool:
        """Update a product's name"""
        products = self.db.products
        savior_id = self.savior_id
        products_created = products.distinct(
            "name", {"savior_id": savior_id}
        )
        print(name_update, products_created)
        if name_update in products_created:
            raise Exception("Product name taken")
        return products.update_many(
            {"savior_id": savior_id, "product_id": ObjectId(product_id)}, 
            {"$set": {"name": name_update}}
        ).acknowledged
        
    def delete_product(self, product_id: str) -> bool:
        return self.db.products.delete_many(
            {"product_id": ObjectId(product_id)}
        ).acknowledged
        
    def handle_file_logs(self, file_logs: list[dict]) -> None:
        import random 
        now = datetime.now()
        savior_id = self.savior_id
        for log in file_logs:
            # if log.keys() & {"value", "activity", "category", "unit"}:
                log.update(
                    {
                        "co2e": random.randint(0, 10), 
                        "savior_id": savior_id, 
                        "created": now
                    }
                )
        print("inserting")
        self.db.logs.insert_many(file_logs)