from kafka import SimpleConsumer as KafkaConsumer
import redis
import json

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Kafka Consumer
consumer = KafkaConsumer("user-interactions", bootstrap_servers="localhost:9092", group_id="ranking_group")

print("Listening for user interactions...")

for message in consumer:
    data = json.loads(message.value.decode("utf-8"))
    video_id = data["video_id"]
    print(f"User interacted with Video {video_id}. Updating ranking...")

    # Update Rankings
    ranked_videos = redis_client.get("ranked_videos")
    if ranked_videos:
        ranked_videos = json.loads(ranked_videos)
        ranked_videos = sorted(ranked_videos, key=lambda x: x[1]["watch_time"], reverse=True)
        redis_client.set("ranked_videos", json.dumps(ranked_videos))
