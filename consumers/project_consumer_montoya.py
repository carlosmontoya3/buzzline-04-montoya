"""
project_consumer_montoya.py

Monitors a JSON-formatted file in real-time and visualizes author message distribution.
"""

#####################################
# Import Modules
#####################################

import json
import os
import sys
import time
import pathlib
from collections import defaultdict

import matplotlib.pyplot as plt
from utils.utils_logger import logger  # Ensure this logger is correctly set up

#####################################
# Set up Paths - Read from the file the producer writes
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("project_live.json")

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Data folder: {DATA_FOLDER}")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Set up data structures
#####################################

author_counts = defaultdict(int)  # Track the count of messages per author
total_messages = 0  # Total message count

#####################################
# Set up live visualization - Bar Chart
#####################################

plt.ion()  # Enable interactive mode for live updates
fig, ax = plt.subplots(figsize=(8, 6))

#####################################
# Update Chart Function
#####################################


def update_chart():
    """Update the live bar chart with the latest author message counts."""
    ax.clear()

    if total_messages == 0:
        return

    authors = list(author_counts.keys())
    counts = list(author_counts.values())

    ax.bar(authors, counts, alpha=0.75)

    ax.set_xlabel("Authors")
    ax.set_ylabel("Message Count")
    ax.set_title("Live Author Message Distribution - Carlos Montoya III")
    ax.set_ylim(0, max(counts) + 2)  # Adjust y-axis for clarity

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.draw()
    plt.pause(0.5)  # Smooth the visualization update


#####################################
# Process Message Function
#####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message and update the chart.

    Args:
        message (str): The JSON message as a string.
    """
    global total_messages

    try:
        message_dict = json.loads(message)

        if not isinstance(message_dict, dict):
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")
            return

        author = message_dict.get("author", "Unknown")  # Default to "Unknown" if missing
        author_counts[author] += 1
        total_messages += 1

        logger.info(f"Total messages: {total_messages}, Author Counts: {dict(author_counts)}")

        update_chart()

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Main Function
#####################################


def main() -> None:
    """
    Main function to monitor a file for new messages and update the live chart.
    """
    logger.info("START consumer.")

    if not DATA_FILE.exists():
        logger.error(f"Data file {DATA_FILE} does not exist. Exiting.")
        sys.exit(1)

    try:
        with open(DATA_FILE, "r") as file:
            file.seek(0, os.SEEK_END)  # Move to the end of the file

            print("Consumer is ready and waiting for new JSON messages...")

            while True:
                line = file.readline()

                if line.strip():
                    process_message(line)
                else:
                    time.sleep(0.5)  # Short delay before checking again

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        plt.ioff()
        plt.show()
        logger.info("Consumer closed.")


#####################################
# Run the Consumer
#####################################

if __name__ == "__main__":
    main()
