"""
project_consumer_pinkston.py

Consume json messages from a Kafka topic and visualize author counts in real-time.

Example Kafka message format:
{"timestamp": "2025-01-11T18:15:00Z", "temperature": 225.0}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
import time

# Use a deque ("deck") - a double-ended queue data structure
# A deque is a good way to monitor a certain number of "most recent" messages
# A deque is a great data structure for time windows (e.g. the last 5 messages)
from collections import deque

# Import external packages
from dotenv import load_dotenv

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("SMOKER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size

def get_high_temp_threshold() -> float:
    threshold = float(os.getenv("SMOKER_HIGH_TEMP_THRESHOLD_F", 90.0))
    logger.info(f"High temp threshold: {threshold}°F")
    return threshold

#####################################
# Set up data structures (empty lists)
#####################################

timestamps = []  # To store timestamps for the x-axis
temperatures = []  # To store temperature readings for the y-axis

#####################################
# Analytics
#####################################

total_messages = 0
high_temps_sum = 0.0
high_temp_count = 0
window_messages = 0
window_start = time.time()

#####################################
# Set up live visuals
#####################################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots()

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################

def update_chart():
    """
    Update temperature vs. time chart.
    Args:
        rolling_window (deque): Rolling window of temperature readings.
        window_size (int): Size of the rolling window.
    """
    # Clear the previous chart
    ax.clear()

    # Fetch the high temp threshold
    high_temp_threshold = get_high_temp_threshold()

    # Create a bar chart
    # Use the timestamps for the x-axis and temperatures for the y-axis
    # Use the label parameter to add a legend entry
    # Use the color parameter to set the bar color
    # Create a seperate bar color for detected high temps
    bar_colors = ["red" if temp >= high_temp_threshold else "blue" for temp in temperatures]
    ax.bar(timestamps, temperatures, color=bar_colors)
    
    # Use the built-in axes methods to set the labels and title
    ax.set_xlabel("Time")
    ax.set_ylabel("Temperature (°F)")
    ax.set_title("Smart Smoker: Temperature vs. Time by James Pinkston")
    plt.xticks(rotation=45)

    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.01)  


#####################################
# Function to process a single message
# #####################################


def process_message(message: str, rolling_window: deque, window_size: int):
    global total_messages, high_temps_sum, high_temp_count, window_messages, window_start
    """
    Process a JSON-transferred CSV message and check for stalls.

    Args:
        message (str): JSON message received from Kafka.
        rolling_window (deque): Rolling window of temperature readings.
        window_size (int): Size of the rolling window.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data = json.loads(message)
        temperature = data.get("temperature")
        timestamp = data.get("timestamp")
        logger.info(f"Processed JSON message: {data}")

        if timestamp is None or temperature is None:
            logger.error(f"Invalid message: {message}")
            return
        
        timestamps.append(timestamp)
        temperatures.append(temperature)

        total_messages += 1
        window_messages += 1
        if temperature >= get_high_temp_threshold():
            high_temps_sum += temperature
            high_temp_count += 1

        # Update chart
        update_chart()

        # Periodic analytics report
        elapsed = time.time() - window_start
        if elapsed >= get_rolling_window_size():
            avg_high_temp = high_temps_sum / high_temp_count if high_temp_count else 0.0
            print("\n=== Analytics ===")
            print(f"Total messages so far: {total_messages}")
            print(f"Average high temp so far: {avg_high_temp:.2f}°F")
            print("===================\n")

            window_messages = 0
            window_start = time.time()

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")

#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls messages and updates a live chart.
    """
    logger.info("START consumer.")

    # Clear previous run's data
    timestamps.clear()
    temperatures.clear()

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    
    rolling_window = deque(maxlen=window_size)

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
    plt.ioff()  # Turn off interactive mode after completion
    plt.show()