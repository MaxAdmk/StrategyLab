import pandas as pd
from datetime import datetime
import json
import os
from output_strategies.console_output import ConsoleOutput
from output_strategies.kafka_output import KafkaOutput
from output_strategies.redis_output import RedisOutput

with open('config.json') as f:
    config = json.load(f)

data_file_path = os.path.join(os.path.dirname(__file__), 'air_quality_testing_data.csv')

df = pd.read_csv(data_file_path)
data = df[:21].copy()

for col in data.columns:
    data[col] = data[col].apply(lambda x: x.isoformat() if isinstance(x, (pd.Timestamp, datetime)) else x)

data = data.to_dict(orient='records')

strategy_map = {
    "console": ConsoleOutput(),
    "kafka": KafkaOutput(config.get("kafka_config", {})),
    "redis": RedisOutput(config.get("redis_config", {}))
}

selected_strategy = strategy_map.get(config.get("output_strategy", "console"))
selected_strategy.write(data)
