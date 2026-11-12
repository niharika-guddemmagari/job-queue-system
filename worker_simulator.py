
import time
import re


class WorkerSimulator:
    def __init__(self, worker_id):
        self.worker_id = worker_id

    def execute_job(self, job):
        job_id = job["job_id"]
        description = job["description"]

        print(
            f"[{self.worker_id}] Executing job {job_id}: "
            f"{description}"
        )

        try:
            # Simulate processing time.
            time.sleep(1)

            result = self._calculate(description)

            print(
                f"[{self.worker_id}] Job {job_id} completed: "
                f"{result}"
            )

            return {
                "job_id": job_id,
                "worker_id": self.worker_id,
                "status": "COMPLETED",
                "result": result,
                "error": None,
            }

        except (ValueError, ZeroDivisionError) as exc:
            print(
                f"[{self.worker_id}] Job {job_id} failed: {exc}"
            )

            return {
                "job_id": job_id,
                "worker_id": self.worker_id,
                "status": "FAILED",
                "result": None,
                "error": str(exc),
            }

    def _calculate(self, description):
        parts = description.strip().upper().split()

        if len(parts) != 3:
            raise ValueError(
                "Use the format: ADD 20 30, SUB 20 5, "
                "MUL 4 5, or DIV 20 4"
            )

        operation, first, second = parts

        # Accept numbers only; never execute arbitrary commands.
        number_pattern = r"-?\d+(?:\.\d+)?"

        if not re.fullmatch(number_pattern, first):
            raise ValueError("First operand must be a number")

        if not re.fullmatch(number_pattern, second):
            raise ValueError("Second operand must be a number")

        a = float(first)
        b = float(second)

        if operation == "ADD":
            result = a + b
        elif operation == "SUB":
            result = a - b
        elif operation == "MUL":
            result = a * b
        elif operation == "DIV":
            if b == 0:
                raise ZeroDivisionError("Cannot divide by zero")
            result = a / b
        else:
            raise ValueError(f"Unsupported operation: {operation}")

        # Return integers without a decimal part.
        if result.is_integer():
            return int(result)

        return result
