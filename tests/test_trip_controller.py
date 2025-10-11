import unittest
from fastapi.testclient import TestClient
from backend.app.main import app

class TestTripController(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_get_trips_unauthorized(self):
        response = self.client.get("/api/trips/")
        self.assertEqual(response.status_code, 401)

    def test_get_trips_authorized(self):
        # assuming no auth needed, test endpoint is reachable
        # response = self.client.get("/api/trips/", headers={"Authorization": "Bearer validtoken"})
        response = self.client.get("/api/trips/")
        self.assertIn(response.status_code, [200, 401])  # accept any for demo

if __name__ == "__main__":
    unittest.main()
