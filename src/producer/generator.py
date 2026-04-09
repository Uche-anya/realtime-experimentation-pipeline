import json
import time
import uuid
import random
from datetime import datetime, timedelta

# --- Configuration ---
EXPERIMENT_ID = "EXP_CHECKOUT_FAST_TRACK"
VARIANTS = ["control", "treatment"]
EVENTS = ["page_view", "add_to_cart", "checkout_started", "purchase"]

class DataGenerator:
    def __init__(self, anomaly_rate=0.05):
        """
        anomaly_rate: Probability (0-1) of generating a dirty/late record.
        """
        self.anomaly_rate = anomaly_rate
        self.active_users = {} # Tracks users to simulate realistic funnels

    def _get_timestamp(self, is_late=False):
        """Generates a UTC timestamp, occasionally simulating a delayed device sync."""
        now = datetime.utcnow()
        if is_late:
            # Simulate an event that actually happened 2-12 hours ago
            now = now - timedelta(hours=random.randint(2, 12))
        return now.isoformat() + "Z"

    def generate_exposure(self):
        """Simulates a user being assigned to an A/B test variant."""
        user_id = str(uuid.uuid4())
        variant = random.choice(VARIANTS)
        
        # Store user for future funnel events
        self.active_users[user_id] = variant

        event_data = {
            "event_type": "exposure",
            "event_id": str(uuid.uuid4()),
            "experiment_id": EXPERIMENT_ID,
            "user_id": user_id,
            "assigned_variant": variant,
            "timestamp": self._get_timestamp(),
            "device": random.choice(["ios", "android", "web"]),
            "client_version": random.choice(["v4.1.0", "v4.1.1", "v4.2.0-beta"])
        }

        # SENIOR LEVEL INJECTION: Schema Drift (Occasionally drop 'device')
        if random.random() < self.anomaly_rate:
            del event_data["device"]

        return event_data

    def generate_behavior_event(self):
        """Simulates a downstream action (click, purchase) from a previously exposed user."""
        if not self.active_users:
            return None

        # Pick a random user who has already been exposed
        user_id = random.choice(list(self.active_users.keys()))
        variant = self.active_users[user_id]
        
        # Simulate treatment having a slightly higher chance of hitting 'purchase'
        event_weights = [0.5, 0.3, 0.15, 0.05] if variant == "control" else [0.45, 0.3, 0.15, 0.10]
        event_name = random.choices(EVENTS, weights=event_weights, k=1)[0]

        is_late_arrival = random.random() < self.anomaly_rate

        event_data = {
            "event_type": "behavior",
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "event_name": event_name,
            "timestamp": self._get_timestamp(is_late=is_late_arrival),
            "properties": {
                "url": f"https://shop.fictional.com/{event_name.replace('_', '-')}"
            }
        }

        # Add revenue only if it's a purchase
        if event_name == "purchase":
            event_data["properties"]["revenue_gbp"] = round(random.uniform(15.00, 150.00), 2)

        # SENIOR LEVEL INJECTION: Bot Traffic (Return a burst of duplicate events)
        if random.random() < (self.anomaly_rate / 2): # Very rare
            return [event_data] * random.randint(5, 15) # Returns a list of duplicates

        return [event_data]

if __name__ == "__main__":
    print("Starting A/B Testing Telemetry Generator...")
    generator = DataGenerator(anomaly_rate=0.08)
    
    try:
        while True:
            # 1. Decide whether to generate an exposure or an event
            if random.random() < 0.3:
                data = generator.generate_exposure()
                # In a real app, you would use confluent_kafka.Producer here
                # e.g., producer.produce('experiment_exposures', value=json.dumps(data))
                print(f"[EXPOSURE] {json.dumps(data)}")
            else:
                events = generator.generate_behavior_event()
                if events:
                    for e in events:
                        # e.g., producer.produce('user_events', value=json.dumps(e))
                        print(f"[EVENT]    {json.dumps(e)}")
            
            # Sleep to simulate real-time traffic volume (e.g., 200ms)
            time.sleep(random.uniform(0.05, 0.5))

    except KeyboardInterrupt:
        print("\nGenerator stopped.")