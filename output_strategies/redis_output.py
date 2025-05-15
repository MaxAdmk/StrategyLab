from .base import OutputStrategy
import json
import redis

class RedisOutput(OutputStrategy):
    def __init__(self, config):
        self.redis = redis.Redis(
            host=config.get("host", "localhost"),
            port=config.get("port", 6379),
            db=config.get("db", 0),
            decode_responses=True
        )

    def write(self, data):
        for row in data:
            date = row.get("Date", "unknown_date")
            location = row.get("Sample Location", "unknown_location")
            key = f"{date}_{location}".replace(" ", "_") 

            value = json.dumps(row, ensure_ascii=False, indent=2)
            self.redis.set(key, value)
            print(f"[Redis] Stored key: {key}")


