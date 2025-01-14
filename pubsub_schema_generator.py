import time
import json
from google.cloud import pubsub_v1
import os

def infer_json_schema(messages):
    """
    Infers an Avro schema from a list of JSON messages.
    """
    def infer_field_type(value):
        """
        Infers the Avro type of a field value.
        """
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "long"
        elif isinstance(value, float):
            return "double"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, list):
            return {"type": "array", "items": "string"}  # Assume array items are strings
        elif isinstance(value, dict):
            return "record"
        elif value is None:
            return "null"
        return "string"

    # Initialize schema fields
    fields = {}
    for message in messages:
        for key, value in message.items():
            if key not in fields:
                fields[key] = [infer_field_type(value)]
            elif infer_field_type(value) not in fields[key]:
                fields[key].append(infer_field_type(value))
    return fields

def generate_avro_schema(fields):
    """
    Converts inferred fields to an Avro schema.
    """
    return {
        "type": "record",
        "name": "PubSubMessage",
        "fields": [
            {"name": key, "type": types if len(types) > 1 else types[0]}
            for key, types in fields.items()
        ],
    }

def main():
    # Configuration
    subscription_name = "projects/project-id/subscriptions/sub-name"
    max_duration = 300  # 5 minutes in seconds
    output_file = "schema.avsc"

    # Initialize Pub/Sub subscriber
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        "account-id", "subscription"
    )

    start_time = time.time()
    received_messages = []

    def callback(message):
        try:
            decoded_message = json.loads(message.data.decode("utf-8"))
            received_messages.append(decoded_message)
            message.ack()
        except json.JSONDecodeError:
            print("Error decoding message:", message.data)

    # Subscribe to the subscription
    future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    try:
        while time.time() - start_time < max_duration:
            time.sleep(1)
            if not future.running():
                break
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        future.cancel()

    if received_messages:
        # Infer schema
        fields = infer_json_schema(received_messages)
        avro_schema = generate_avro_schema(fields)

        # Write schema to file
        with open(output_file, "w") as f:
            json.dump(avro_schema, f, indent=2)
        print(f"Schema written to {output_file}")
    else:
        print("No messages received. No schema inferred.")

if __name__ == "__main__":
    main()
