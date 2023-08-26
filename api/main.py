#!/usr/bin/env
"""FastAPI application that handles sending and receiving requests to MongoDB using Motor driver"""
from local_variables import mongo_conn, app_db_name
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

app = FastAPI()


async def mongo_db(db_name):
    """Connect to MongoDB using Motor driver. The client connection path for this application
    is defined in ./local_variables file and the database name is given via method arguments.
    """
    motor_client = AsyncIOMotorClient(mongo_conn)
    motor_db = motor_client[db_name]
    return motor_db


@app.get("/teams")
async def get_teams():
    """Defines '/teams' endpoint that fetches data from teams collection and performs an
    aggregation to join it with users collection based on members filed, sorts the users
    by age in ascending order and groups them into teams."""
    motor_db = await mongo_db(app_db_name)
    teams_coll = motor_db["teams"]
    pipeline = [
        {
            "$lookup": {
                "from": "users",
                "localField": "members",
                "foreignField": "_id",
                "as": "members",
            }
        },
        {"$unwind": "$members"},
        {"$sort": {"members.age": 1}},
        {
            "$group": {
                "_id": "$_id",
                "title": {"$first": "$title"},
                "members": {"$push": "$members"},
            }
        },
        {"$project": {"title": 1, "members.name": 1, "members.age": 1, "_id": 0}},
    ]
    data = await teams_coll.aggregate(pipeline).to_list(None)
    return data


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port="8080")
