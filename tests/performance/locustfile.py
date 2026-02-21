"""
Locust performance testing script for SOUL_SENSE_EXAM API
Tests API performance under concurrent user load
"""

from locust import HttpUser, task, between, events
from locust.runners import MasterRunner
import json
import random


class SoulSenseUser(HttpUser):
    """Simulated user behavior for load testing"""

    wait_time = between(1, 3)

    def on_start(self):
        """Login on start"""
        self.client.verify = False
        username = f"loadtest_{random.randint(1000, 9999)}"
        password = "TestPass123!"

        response = self.client.post(
            "/api/v1/auth/register",
            json={
                "username": username,
                "password": password,
                "email": f"{username}@example.com"
            }
        )

        if response.status_code == 201:
            self.token = response.json()["access_token"]
            self.headers = {"Authorization": f"Bearer {self.token}"}
        else:
            login_response = self.client.post(
                "/api/v1/auth/login",
                data={"username": username, "password": password}
            )
            if login_response.status_code == 200:
                self.token = login_response.json()["access_token"]
                self.headers = {"Authorization": f"Bearer {self.token}"}
            else:
                self.headers = {}

    @task(3)
    def get_questions(self):
        """Fetch questions - high frequency task"""
        if hasattr(self, 'headers'):
            self.client.get(
                "/api/v1/questions?age=25&limit=10",
                headers=self.headers
            )

    @task(2)
    def get_journal_entries(self):
        """Fetch journal entries - medium frequency task"""
        if hasattr(self, 'headers'):
            self.client.get("/api/v1/journal", headers=self.headers)

    @task(1)
    def create_journal_entry(self):
        """Create journal entry - low frequency task"""
        if hasattr(self, 'headers'):
            moods = ["happy", "sad", "neutral", "excited", "grateful"]
            self.client.post(
                "/api/v1/journal",
                headers=self.headers,
                json={
                    "content": "Performance test entry",
                    "mood": random.choice(moods)
                }
            )

    @task(1)
    def get_user_profile(self):
        """Fetch user profile - low frequency task"""
        if hasattr(self, 'headers'):
            self.client.get("/api/v1/users/me", headers=self.headers)

    @task(1)
    def get_assessment_history(self):
        """Fetch assessment history - low frequency task"""
        if hasattr(self, 'headers'):
            self.client.get("/api/v1/assessments/history", headers=self.headers)


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Cleanup after test"""
    if isinstance(environment.runner, MasterRunner):
        print("Performance test completed")
        print(f"Total requests: {environment.runner.stats.total.num_requests}")
        print(f"Failures: {environment.runner.stats.total.num_failures}")
        print(f"Median response time: {environment.runner.stats.total.median_response_time}ms")
        print(f"Average response time: {environment.runner.stats.total.avg_response_time}ms")
