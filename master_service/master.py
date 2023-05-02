import requests
import time
import csv
import threading
from datetime import datetime

PNGM_ENDPOINTS = [
    "http://localhost:8001/prime_app",
    "http://localhost:8002/prime_app",
    "http://localhost:8003/prime_app",
]

def distribute_work():
    print("Distributing work...")
    work_range = (1, 20)  # Reduced range to generate prime numbers faster
    step = work_range[1] // 3

    for i, endpoint in enumerate(PNGM_ENDPOINTS):
        from_num = work_range[0] + (step * i)
        to_num = work_range[0] + (step * (i + 1)) - 1
        try:
            requests.get(f"{endpoint}/generate?from={from_num}&to={to_num}")
        except requests.exceptions.RequestException as e:
            print(f"Error sending work to {endpoint}: {e}")


def log_resource_utilization():
    print("Starting resource logging thread...")
    with open("resource_utilization.csv", mode="w", newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["timestamp", "cpu", "memory"])

        while True:
            try:
                response = requests.get(f"{PNGM_ENDPOINTS[0]}/monitor?k=1")
                cpu = response.json()["cpu_percent"]
                memory = response.json()["memory_percent"]
                timestamp = datetime.now().strftime("%m-%d-%Y %H:%M:%S")

                csvwriter.writerow([timestamp, cpu, memory])
                csvfile.flush()
                print(f"Logged resource utilization: {timestamp}, CPU: {cpu}, Memory: {memory}")
            except requests.exceptions.RequestException as e:
                print(f"Error retrieving resource utilization: {e}")

            time.sleep(60)

def update_prime_numbers():
    print("Starting prime number updating thread...")
    master_prime_list = []

    while True:
        for endpoint in PNGM_ENDPOINTS:
            try:
                response = requests.get(f"{endpoint}/get")
                prime_numbers = response.json()["prime_numbers"]
                master_prime_list.extend(prime_numbers)
                master_prime_list = sorted(set(master_prime_list))
            except requests.exceptions.RequestException as e:
                print(f"Error retrieving prime numbers from {endpoint}: {e}")

        print("Updated prime numbers:", master_prime_list)
        time.sleep(120)

def main():
    distribute_work()
    time.sleep(60)

    resource_logging_thread = threading.Thread(target=log_resource_utilization)
    prime_number_updating_thread = threading.Thread(target=update_prime_numbers)

    resource_logging_thread.start()
    prime_number_updating_thread.start()

    resource_logging_thread.join()
    prime_number_updating_thread.join()

if __name__ == "__main__":
    main()
