from quixstreams import Application
from quixstreams.dataframe.sql import StreamingSQLExecutor
import uuid

import os
from dotenv import load_dotenv
from datetime import timedelta

load_dotenv()

app = Application(consumer_group="heatmap-aggregator-v1.1", auto_offset_reset="earliest")

input_topic = app.topic(os.environ["input"])
sdf = app.dataframe(input_topic)

def get_tokens_count(message: str):
    print(message)
    return len(message.split(" "))

#sdf = sdf.sql("SELECT COUNT(relative_path) FROM __self WHERE relative_path like '/'")

sdf = sdf.sql(
"""
SELECT TUMBLE_START(__timestamp, INTERVAL '1' MINUTE) AS window_start,
          COUNT(speed) AS n_rows
FROM __self
GROUP BY TUMBLE(__timestamp, INTERVAL '1' MINUTE);
"""
    )

#sdf["tokens_count"] = sdf["message"].apply(lambda message: len(message.split(" ")))
#sdf = sdf[["role", "tokens_count"]]

sdf = sdf.update(lambda row: print(row))

app.run(sdf)