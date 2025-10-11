# NYC Train Mobility Backend

This backend service provides RESTful APIs for managing NYC Train mobility data, including trips and vendors. It includes data processing, JWT-based authentication, and CRUD endpoints.

---

## Setup Instructions

1. **Clone the repository:**

   ```
   git clone <repository-url>
   cd backend
   ```

2. **Create and activate a virtual environment:**

   ```
   python3 -m venv venv
   source venv/bin/activate  # Linux/macOS
   venv\Scripts\activate     # Windows
   ```

3. **Install dependencies:**

   ```
   pip install -r requirements.txt
   ```

4. **Run the data processing pipeline to clean and prepare data:**

   ```
   python data_processing.py
   ```

   This will load raw data from `data/raw/train.zip`, clean and enrich it, and save to `data/processed/clean_train.csv`.

5. **Start the FastAPI server:**

   ```
   uvicorn backend.app.main:app --reload
   ```

6. **Access API docs:**

   Open [http://localhost:8000/docs](http://localhost:8000/docs) for interactive Swagger UI documentation.

---

## API Endpoints Documentation

### Authentication

| Endpoint          | Method | Description                              | Request Body                       | Response                         |
|-------------------|--------|------------------------------------------|------------------------------------|----------------------------------|
| `/auth/token`     | POST   | Obtain JWT access token                  | `username`, `password` (form-data) | `access_token` and `token_type`  |

---

### Vendors

| Endpoint          | Method | Description                              | Request Body                       | Response                         |
|-------------------|--------|------------------------------------------|------------------------------------|----------------------------------|
| `/vendors/`       | GET    | Get list of all vendors (Auth required)  | None                               | List of vendor objects           |
| `/vendors/{id}`   | GET    | Get vendor details by ID (Auth required) | None                               | Vendor object                    |
| `/vendors/`       | POST   | Create new vendor (Auth required)        | `{ "vendor_name": "string" }`      | Created vendor object            |
| `/vendors/{id}`   | PUT    | Update vendor (Auth required)            | `{ "vendor_name": "string" }`      | Updated vendor object            |
| `/vendors/{id}`   | DELETE | Delete vendor (Auth required)            | None                               | No content                       |

---

### Trips

| Endpoint          | Method | Description                                               | Request Body                        | Response                         |
|-------------------|--------|-----------------------------------------------------------|-------------------------------------|----------------------------------|
| `/trips/`         | GET    | List trips, optional filter by vendor ID (Auth required)  | Optional query param: `vendor_id`   | List of trip objects             |
| `/trips/{id}`     | GET    | Get trip by ID (Auth required)                            | None                                | Trip object                      |
| `/trips/`         | POST   | Create new trip (Auth required)                           | Full trip details JSON as per schema| Created trip object              |
| `/trips/{id}`     | PUT    | Update trip details (Auth required)                       | Full trip details JSON as per schema| Updated trip object              |
| `/trips/{id}`     | DELETE | Delete trip (Auth required)                               | None                                | No content                       |

---

## Data Processing

The `data_processing.py` script includes:

- Loading raw data from CSV in ZIP archives
- Cleaning timestamp, numeric, and categorical data
- Deriving features: `trip_speed_kmh`, `trip_efficiency`, `idle_time_sec`, `fare_per_km`
- Detecting speed outliers using a custom linked list data structure for anomaly detection
- Saving cleaned data to `data/processed/clean_train.csv`

Run this before starting the API server to work with clean data.

---

## Running Tests

*(Add instructions if tests are implemented)*

---

## Notes

- All endpoints (except authentication) require a valid JWT token via Authorization header.
- CORS settings allow requests from localhost frontend ports.
- Use the interactive Swagger UI `/docs` for easy testing and exploration.

---

## Contact

For questions or support, contact the project maintainer.

