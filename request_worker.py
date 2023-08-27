# -*- coding:utf-8 -*-
# @FileName  :request_worker.py
# @Time      :27/8/2023 11:28 pm
# @Author    :Xiaotian

import requests
import json
from time import sleep

api_endpoint = "http://localhost:3001/buildingDataValueHourly/autoTriggerHourlyCalculation"


def send_request(segment):
    """Sends a segment request to the API endpoint."""
    print(f"Sending segment:\n{json.dumps(segment, indent=4)}")

    try:
        print(f"working...")
        response = requests.post(api_endpoint, json=segment)
        # Check the response for errors
        if response.json()['message'] == 'success':
            print(f"Success for segment {segment['startTime']} to {segment['endTime']}")
            print(f"sleep 5 sec")
            sleep(5)
            return None  # Success, return None
        else:
            print(f"Error for segment {segment['startTime']} to {segment['endTime']}: {response.text}")
            print(f"sleep 30 sec")
            sleep(30)  # Wait for 60 seconds before next request
            return segment  # Return failed segment
    except Exception as e:
        print(f"Exception for segment {segment['startTime']} to {segment['endTime']}: {e}")
        print(f"sleep 30 sec")
        sleep(30)  # Wait for 60 seconds before next request
        return segment  # Return failed segment
