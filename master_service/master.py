import requests
import time
import csv
from datetime import datetime

PNGM_ENDPOINTS = [
    "http://localhost:8001/prime_app",
    "http://localhost:8002/prime_app",
    "http://localhost:8003/prime_app",
]

def distribute_work():
    work_range = (1, 10**12)
    step = work_range[1] // 3

    for i, endpoint in enumerate(PNGM_ENDPOINTS):
        from_num = work_range[0] + (step * i)
        to_num = work_range[0] + (step * (i + 1)) - 1
        requests.get(f"{endpoint}/generate?from={from_num}&to={to_num}")

def log_resource_utilization():
    with open("resource_utilization.csv", mode="w", newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["timestamp", "cpu", "memory"])

        while True:
            response = requests.get(f"{PNGM_ENDPOINTS[0]}/monitor?k=1")
            cpu = response.json()["cpu_percent"]
            memory = response.json()["memory_percent"]
            timestamp = datetime.now().strftime("%m-%d-%Y %H:%M:%S")

            csvwriter.writerow([timestamp, cpu, memory])
            csvfile.flush()
            time.sleep(60)

def update_prime_numbers():
    master_prime_list = []

    while True:
        for endpoint in PNGM_ENDPOINTS:
            response = requests.get(f"{endpoint}/get")
            prime_numbers = response.json()["prime_numbers"]
            master_prime_list.extend(prime_numbers)
            master_prime_list = sorted(set(master_prime_list))

        print("Updated prime numbers:", master_prime_list)
        time.sleep(120)

def main():
    distribute_work()
    log_resource_utilization()
    update_prime_numbers()

if __name__ == "__main__":
    main()
