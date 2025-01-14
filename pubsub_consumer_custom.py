from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
import json

# Configuration
project_id = "project-id"  # Client project ID
subscription_id = "sub-id"  # Your subscription ID
MAX_MESSAGES = 10

# Authentication
# Export the credentials using the following command before running the script
# export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

received_messages = []

def callback(message):
    try:
        message_data = json.loads(message.data.decode("utf-8"))
        event_name = message_data.get("eventName")
        # Only process messages not matching the excluded event names
        # check gdprterminated it was having null payload for some reason
        if event_name not in {"AccountCreated", "CommChannelAdded", "CommChannelRemoved", "AccountTerminated", "AccountDeleted", "ShfUpdated", "GDPRDeleted", "GDPRTerminated", "CommChannelDisavowed"}:
            print(f"Received message: {message.data}")
            received_messages.append(message)
        message.ack()
    except json.JSONDecodeError:
        print(f"Failed to decode message: {message.data}")
        message.ack()

    if len(received_messages) >= MAX_MESSAGES:
        future.cancel()

future = subscriber.subscribe(subscription_path, callback=callback)

print(f"Listening for messages on {subscription_path}...")

# Timeout of 30 seconds to receive messages, or until MAX_MESSAGES are received
try:
    future.result(timeout=300)
except TimeoutError:
    print("Timeout reached or max messages received.")
except KeyboardInterrupt:
    print("Subscriber manually stopped.")
finally:
    future.cancel()

print(f"Received {len(received_messages)} messages.")
