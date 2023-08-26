import datetime
from time import sleep

import requests
import json

if __name__ == '__main__':

    ################ CONFIGURATION ################
    api_endpoint = "http://localhost:3001/buildingDataValueHourly/autoTriggerHourlyCalculation"
    site_name = "warehouse-dandenong-south"
    start_time = datetime.datetime.fromisoformat("2023-04-30T14:00:00.000")
    end_time = datetime.datetime.fromisoformat("2023-05-31T13:59:00.000")
    delta = datetime.timedelta(hours=1)
    # Smallest delta to prevent start_time and end_time overlap between segments
    epsilon = datetime.timedelta(minutes=1)
    # if enable, each request will be sent after user confirm
    enable_manual_continue = True  # Set to True to enable manual input
    ####################   END   #################

    # Generate time segments
    time_segments = []
    current_time = start_time
    while current_time < end_time:
        next_time = current_time + delta
        # Adjust the next_time if it exceeds the end_time
        if next_time > end_time:
            next_time = end_time

        segment = {
            "site": site_name,
            "startTime": current_time.isoformat() + 'Z',
            "endTime": next_time.isoformat() + 'Z'
        }
        time_segments.append(segment)

        current_time = next_time + epsilon

    failed_requests = []  # List to store failed requests

    # Send each segment to API
    for segment in time_segments:
        print(f"Sending segment:\n{json.dumps(segment, indent=4)}")
        if enable_manual_continue:
            input("Press enter to proceed segment...")  # Wait for user input

        try:
            print(f"working...")
            response = requests.post(api_endpoint, json=segment)
            # Check the response for errors
            if response.json()['message'] == 'success':
                print(f"Success for segment {segment['startTime']} to {segment['endTime']}")
                sleep(10)
            else:
                print(f"Error for segment {segment['startTime']} to {segment['endTime']}: {response.text}")
                failed_requests.append(segment)  # Add failed segment to the list
                sleep(60)  # Wait for 60 seconds before next request
        except Exception as e:
            print(f"Exception for segment {segment['startTime']} to {segment['endTime']}: {e}")
            failed_requests.append(segment)  # Add failed segment to the list
            sleep(60)  # Wait for 60 seconds before next request

    print("All segments processed!")

    # Export failed requests to a JSON file
    if failed_requests:
        with open("failed_requests.json", "w") as file:
            json.dump(failed_requests, file, indent=4)
        print("Failed requests exported to 'failed_requests.json'")
