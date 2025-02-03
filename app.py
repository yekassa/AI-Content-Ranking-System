from flask import Flask, request, jsonify
import redis
import json
from kafka import SimpleProducer as KafkaProducer

app = Flask(__name__)

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Dummy Video Data
videos = {
    "1": {"title": "Video 1", "watch_time": 10, "likes": 5, "shares": 2, "comments": 3},
    "2": {"title": "Video 2", "watch_time": 15, "likes": 8, "shares": 1, "comments": 5},
    "3": {"title": "Video 3", "watch_time": 20, "likes": 15, "shares": 5, "comments": 10},
}

# Engagement Scoring Function
def compute_engagement_score(video):
    return (video["watch_time"] * 0.5 +
            video["likes"] * 0.2 +
            video["comments"] * 0.15 +
            video["shares"] * 0.15)

# Rank Videos & Store in Redis
def rank_videos():
    ranked_videos = sorted(videos.items(), key=lambda x: compute_engagement_score(x[1]), reverse=True)
    redis_client.set("ranked_videos", json.dumps(ranked_videos))

# API Endpoint: Get Ranked Videos
@app.route('/get_ranked_videos', methods=['GET'])
def get_ranked_videos():
    ranked_videos = redis_client.get("ranked_videos")
    if ranked_videos:
        return jsonify(json.loads(ranked_videos))
    return jsonify([])

# API Endpoint: User Interaction
@app.route('/user_interaction', methods=['POST'])
def user_interaction():
    data = request.json
    video_id = data["video_id"]
    interaction_type = data["interaction_type"]

    # Store interaction in Kafka
    producer.send("user-interactions", json.dumps(data).encode())

    # Update engagement score
    if video_id in videos:
        if interaction_type == "like":
            videos[video_id]["likes"] += 1
        elif interaction_type == "comment":
            videos[video_id]["comments"] += 1
        elif interaction_type == "share":
            videos[video_id]["shares"] += 1
        elif interaction_type == "watch_time":
            videos[video_id]["watch_time"] += data.get("time", 0)

    rank_videos()
    return jsonify({"message": "Interaction recorded!"})

if __name__ == '__main__':
    rank_videos()
    app.run(debug=True, port=5000)
