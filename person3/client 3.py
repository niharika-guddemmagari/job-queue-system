"""Example client. Run: python client.py"""
import json
import socket

MANAGER_HOST = "127.0.0.1"
MANAGER_PORT = 5000


def request(message):
    with socket.create_connection((MANAGER_HOST, MANAGER_PORT), timeout=5) as sock:
        sock.sendall((json.dumps(message) + "\n").encode("utf-8"))
        file_obj = sock.makefile("r", encoding="utf-8")
        line = file_obj.readline()
        if not line:
            raise ConnectionError("Manager returned no response.")
        return json.loads(line)


def main():
    print("1. Submit sum job")
    print("2. Submit factorial job")
    print("3. Submit max job")
    print("4. Check job status/result")
    print("5. List workers")
    choice = input("Choose: ").strip()

    if choice == "1":
        start = int(input("Start integer: "))
        end = int(input("End integer: "))
        message = {"type": "submit_job", "job_type": "sum",
                   "payload": {"start": start, "end": end}}
    elif choice == "2":
        n = int(input("n (0..500): "))
        message = {"type": "submit_job", "job_type": "factorial", "payload": {"n": n}}
    elif choice == "3":
        values = [float(x.strip()) for x in input("Numbers separated by commas: ").split(",") if x.strip()]
        message = {"type": "submit_job", "job_type": "max", "payload": {"values": values}}
    elif choice == "4":
        job_id = input("Job ID: ").strip()
        message = {"type": "get_status", "job_id": job_id}
    elif choice == "5":
        message = {"type": "list_workers"}
    else:
        print("Invalid choice.")
        return

    print(json.dumps(request(message), indent=2))


if __name__ == "__main__":
    main()
