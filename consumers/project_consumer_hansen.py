"""
project_consumer_hansen.py

Consume json messages from a JSON file and visualize population trends.

JSON is a set of key:value pairs. 

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting author occurrences
from itertools import count

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
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up data structures
#####################################

# Sample JSON messages
messages = [
    '{"indicator": {"id": "SP.POP.TOTL", "value": "Population, total"}, "country": {"id": "US", "value": "United States"}, "countryiso3code": "USA", "date": "2020", "value": 331526933, "unit": "", "obs_status": "", "decimal": 0}',
    '{"indicator": {"id": "SP.POP.TOTL", "value": "Population, total"}, "country": {"id": "US", "value": "United States"}, "countryiso3code": "USA", "date": "2019", "value": 328329953, "unit": "", "obs_status": "", "decimal": 0}',
    '{"indicator": {"id": "SP.POP.TOTL", "value": "Population, total"}, "country": {"id": "US", "value": "United States"}, "countryiso3code": "USA", "date": "2018", "value": 326838199, "unit": "", "obs_status": "", "decimal": 0}'
]

# Initialize a dictionary to store year, population, year counts, and average population
year_list = defaultdict(int)        # for storing years
population_list=defaultdict(int)    # for population
year_count = defaultdict(int)      # for year counts   
avg_population = defaultdict(float) # for average population

years = []
populations = []

index = count()

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


def update_chart(year, population):
    """Update the live chart with the population data."""
    years.append(year)
    populations.append(population)
    year_count[year] += 1
    population_list[year] += population
    avg_population[year] = population_list[year] / year_count[year]
    
    # Clear the previous chart
    ax.clear()

    # Create a line chart using the plot() method.
    ax.plot(years, populations, label='Population', color="grey")
    ax.plot(years, [avg_population[year] for year in years], label='Average Population')

    # Use the built-in axes methods to set the labels and title
    ax.set_xlabel("Year")
    ax.set_ylabel("Population")
    ax.set_title("United States Population and Average Population Trend")

    # Use the set_xticklabels() method to rotate the x-axis labels
    # Pass in the x list, specify the rotation angle is 45 degrees,
    # and align them to the right
    # ha stands for horizontal alignment
    ax.set_xticks(years)
    ax.set_xticklabels(years, rotation=45, ha="right")

    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.01)

# Print the results
print("Year List:", dict(year_list))
print("Population List:", dict(population_list))
print("Year Count:", dict(year_count))
print("Average Population:", dict)
#####################################
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka and update the chart.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict = json.loads(message)

        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed JSON message: {message_dict}")

        # Ensure it's a dictionary before accessing fields
        if isinstance(message_dict, dict):
            # Extract the year and population field from the Python dictionary
            data = json.loads(message)
            year = int(data['date'])
            population = data['value']
        
            if year and population:
                year_list[year] = year
                population_list[year] += population
                year_count[year] += 1

            # Log the updated counts
            logger.info(f"Updated year counts: {dict(year_count)}")

            # Update the chart
            update_chart(year, populations)

            # Log the updated chart
            logger.info(f"Chart updated successfully for message: {message}")
        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


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

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            # message is a complex object with metadata and value
            # Use the value attribute to extract the message as a string
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":

    # Call the main function to start the consumer
    main()

    # Turn off interactive mode after completion
    plt.ioff()  

    # Display the final chart
    plt.show()
