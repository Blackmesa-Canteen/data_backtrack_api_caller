import datetime
import threading
import json
from queue import Queue
from request_worker import send_request

################ CONFIGURATION ################
site_name = "warehouse-dandenong-south"
start_time = datetime.datetime.fromisoformat("2023-04-30T14:00:00.000")
end_time = datetime.datetime.fromisoformat("2023-05-31T13:59:00.000")
delta = datetime.timedelta(hours=1)
epsilon = datetime.timedelta(minutes=1)
num_threads = 5  # Number of threads to use
####################   END   #################

def worker(segment_queue, failed_reqs, failed_reqs_lock):
    """
    Worker thread function.
    :return:
    """
    while True:
        segment = segment_queue.get()  # Get a segment from the queue
        if segment is None:  # If None, this signals the thread to exit
            break

        failed_segment = send_request(segment)
        if failed_segment:
            with failed_reqs_lock:
                failed_reqs.append(failed_segment)  # Add failed segment to the list with lock protection
                print(f"Failed request for segment: {failed_segment['startTime']} to {failed_segment['endTime']}")

        segment_queue.task_done()  # Mark the task as done in the queue

if __name__ == '__main__':
    segments_queue = Queue()
    failed_requests = []
    # lock for preventing concurrent update to thread-unsafe list
    failed_requests_lock = threading.Lock()
    threads = []

    # Generate time segments
    time_segments = []
    current_time = start_time
    while current_time < end_time:
        next_time = current_time + delta
        if next_time > end_time:
            next_time = end_time

        segment = {
            "site": site_name,
            "startTime": current_time.isoformat() + 'Z',
            "endTime": next_time.isoformat() + 'Z'
        }
        time_segments.append(segment)

        current_time = next_time + epsilon

    # Put time segments into task queue
    for segment in time_segments:
        segments_queue.put(segment)

    # Set up & run threads
    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(segments_queue, failed_requests, failed_requests_lock))
        t.start()
        threads.append(t)

    # Block until all tasks in the queue are done
    segments_queue.join()

    # Stop worker threads
    for i in range(num_threads):
        segments_queue.put(None)
    for t in threads:
        t.join()

    print("All segments processed!")
