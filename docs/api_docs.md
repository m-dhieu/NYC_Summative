# NYC Train Mobility API Documentation

## Authentication
- Uses JWT Bearer Token Authentication
- Obtain access token via `/api/auth/token` endpoint with valid credentials
- Include `Authorization: Bearer <token>` header in protected requests
- Unauthorized requests respond with 401 status

---

## Interactive API Documentation
For detailed API exploration, after starting the main app, visit the Swagger UI at:

[http://localhost:8000/docs](http://localhost:8000/docs)

---

## Endpoints

### GET /api/vendors/
- Description: Retrieve a list of all vendors
- Request:
  ```
  curl -H "Authorization: Bearer <token>" http://localhost:8000/api/vendors/
  ```
- Response:
  ```
  [
    {"vendor_id": 1, "vendor_name": "Vendor A"},
    {"vendor_id": 2, "vendor_name": "Vendor B"}
  ]
  ```
- Errors:
  - 401 Unauthorized: Missing or invalid token

---

### GET /api/vendors/{id}
- Description: Retrieve details of a specific vendor by ID
- Request:
  ```
  curl -H "Authorization: Bearer <token>" http://localhost:8000/api/vendors/1
  ```
- Response:
  ```
  {"vendor_id": 1, "vendor_name": "Vendor A"}
  ```
- Errors:
  - 401 Unauthorized
  - 404 Not Found: Vendor does not exist

---

### POST /api/vendors/
- Description: Create a new vendor
- Request:
  ```
  curl -X POST -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
       -d '{"vendor_name": "New Vendor"}' http://localhost:8000/api/vendors/
  ```
- Response:
  ```
  {"vendor_id": 3, "vendor_name": "New Vendor"}
  ```
- Errors:
  - 401 Unauthorized
  - 400 Bad Request: Invalid input data

---

### PUT /api/vendors/{id}
- Description: Update an existing vendor by ID
- Request:
  ```
  curl -X PUT -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
       -d '{"vendor_name": "Updated Vendor"}' http://localhost:8000/api/vendors/1
  ```
- Response:
  ```
  {"vendor_id": 1, "vendor_name": "Updated Vendor"}
  ```
- Errors:
  - 401 Unauthorized
  - 404 Not Found: Vendor does not exist
  - 400 Bad Request: Invalid input data

---

### DELETE /api/vendors/{id}
- Description: Delete a vendor by ID
- Request:
  ```
  curl -X DELETE -H "Authorization: Bearer <token>" http://localhost:8000/api/vendors/1
  ```
- Response:
  - 200 OK: Vendor deletion confirmation
- Errors:
  - 401 Unauthorized
  - 404 Not Found: Vendor not found

---

### GET /api/trips/
- Description: Retrieve a list of trips; optionally filter by `vendor_id`
- Request:
  ```
  curl -H "Authorization: Bearer <token>" http://localhost:8000/api/trips/?vendor_id=1
  ```
- Response:
  ```
  [
    {
      "trip_id": 101,
      "vendor_id": 1,
      "pickup_datetime": "2025-10-10T09:00:00",
      "dropoff_datetime": "2025-10-10T09:15:00",
      "passenger_count": 2,
      "trip_duration_sec": 900,
      "trip_distance_km": 3.2,
      "trip_speed_kmh": 12.8,
      "trip_efficiency": 0.11,
      "idle_time_sec": 300,
      "fare_per_km": 4.50
    }
  ]
  ```
- Errors:
  - 401 Unauthorized

---

### GET /api/trips/{id}
- Description: Retrieve trip details by ID
- Request:
  ```
  curl -H "Authorization: Bearer <token>" http://localhost:8000/api/trips/101
  ```
- Response:
  ```
  {
    "trip_id": 101,
    "vendor_id": 1,
    "pickup_datetime": "2025-10-10T09:00:00",
    "dropoff_datetime": "2025-10-10T09:15:00",
    "passenger_count": 2,
    "trip_duration_sec": 900,
    "trip_distance_km": 3.2,
    "trip_speed_kmh": 12.8,
    "trip_efficiency": 0.11,
    "idle_time_sec": 300,
    "fare_per_km": 4.50
  }
  ```
- Errors:
  - 401 Unauthorized
  - 404 Not Found: Trip not found

---

### POST /api/trips/
- Description: Create a new trip record
- Request:
  ```
  curl -X POST -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
       -d '{"vendor_id": 1, "pickup_datetime": "2025-10-10T09:00:00", "dropoff_datetime": "2025-10-10T09:15:00", "passenger_count": 2, "pickup_latitude": 40.7128, "pickup_longitude": -74.0060, "dropoff_latitude": 40.7060, "dropoff_longitude": -74.0086, "fare_amount": 15.0}' \
       http://localhost:8000/api/trips/
  ```
- Response:
  ```
  {"trip_id": 102, "vendor_id": 1, ... }
  ```
- Errors:
  - 401 Unauthorized
  - 400 Bad Request

---

### PUT /api/trips/{id}
- Description: Update an existing trip by ID
- Request:
  ```
  curl -X PUT -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
       -d '{...updated trip JSON...}' http://localhost:8000/api/trips/101
  ```
- Response:
  ```
  {"trip_id": 101, "vendor_id": 1, ... updated fields ...}
  ```
- Errors:
  - 401 Unauthorized
  - 400 Bad Request
  - 404 Not Found

---

### DELETE /api/trips/{id}
- Description: Delete a trip by ID
- Request:
  ```
  curl -X DELETE -H "Authorization: Bearer <token>" http://localhost:8000/api/trips/101
  ```
- Response:
  - 200 OK: Trip deletion confirmation
- Errors:
  - 401 Unauthorized
  - 404 Not Found

---

## Notes
- Replace `<token>` with your valid JWT token.
- API consumes and produces JSON.
- Full API documentation, including models and examples, available at Swagger UI after starting the main app:
  - [Swagger UI](http://localhost:8000/docs)
  - [ReDoc](http://localhost:8000/redoc)

---

For any additional assistance or test collections, contact the project maintainer.
