import time
import json
from azure.eventhub import EventHubProducerClient, EventData

# Replace with your EventHub connection string and name
CONNECTION_STR = 'Endpoint=sb://<eventhub_namespace>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<shared_access_key>'
EVENTHUB_NAME = 'your_eventhub_name'

def send_json_to_eventhub(file_path):
    try:
        producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)
        with open(file_path, 'r') as file:
            json_data = json.load(file)
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json.dumps(json_data)))
            producer.send_batch(event_data_batch)
        producer.close()
        print("Data sent to EventHub successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    json_file_path = 'sample.json'
    max_sends =  5 # Set the number of sends before termination
    send_count = 0

    while send_count < max_sends:
        send_json_to_eventhub(json_file_path)
        send_count += 1
        time.sleep(1)  # Wait for 1 second

    print(f"Terminated after {max_sends} sends.")