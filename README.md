# NYC Train Mobility Application

## Table of Contents

* [Overview](#overview)
* [Features](#features)
* [Demo Video](#demo-video)
* [Live URL](#live-url)
* [Repository Structure](#repository-structure)
* [Architecture](#architecture)
* [Data Processing & Database](#data-processing--database)
* [API Server & Frontend](#api-server--frontend)
* [Docker & Deployment](#docker--deployment)
* [Testing](#testing)
* [Environment](#environment)
* [Team](#team)
* [Setup Instructions](#setup-instructions)
* [Contributing](#contributing)
* [License](#license)
* [Contact](#contact)

---

## Overview

The NYC Train Mobility App provides a comprehensive platform to process, serve, and visualize New York City train mobility data.  
It integrates a robust backend API built with FastAPI, supported by a data processing pipeline, and powered by a modern frontend interface.

---

## Features

* Clean, normalize, and feature engineer trip data from raw sources  
* Persistent storage with a relational SQLite database  
* RESTful API with endpoint documentation (Swagger UI)  
* Containerized backend and frontend deployments via Docker  
* Automated CI/CD builds with GitHub Actions  
* Interactive frontend consuming backend APIs  
* Unit tests for backend logic and API routes  
* Comprehensive JSON schema and example data for API consumers  

---

## Demo Video

[Demo Video](https://youtu.be/your-demo-video-id) showcasing key functionalities and UI walk-through.  

---

## Live URL

[Live URL](mdhieu.tech:8083) accessible via a .tech domain.  

---

## Repository Structure 

```
nyc-train-mobility-app/
├── backend/               # backend API source code
│   ├── app/               # app modules
│   ├── nyc_train.db       # processed data in relational DB form
│   ├── __init__.py
│   ├── requirements.txt   # dependencies
│   ├── .env               # configuration variables (DB host, ports...)
│   ├── Dockerfile         # Docker file for backend container
│   └── README.md          # backend overview & structure explanation
├── data/                  # raw & cleaned CSV files                 
├── frontend/              # frontend static assets
│   ├── cs/
│   └── js/
├── index.html             # frontend landing page
├── docs/                  # documentation files
├── examples/              # examples (JSON schemas for trip data, & sample input data)      
├── tests/                 # automated test scripts and cases
├── .github/               # GitHub Actions workflow for CI/CD
├── docker-compose.yml     # Docker Compose file defining multi-container setup
├── .dockerignore          # Files/folders Docker should ignore
├── .gitignore             # Files/folders Git should ignore
├── README.md              # project overview & instructions
└── CONTRIBUTING.md        # contributing guidelines
```

---

## Architecture

- Data pipeline ingests raw trip data → Cleans, derives features → Loads into SQLite DB  
- Backend REST API serves vendor and trip data securely  
- Frontend SPA fetches API data for interactive display  
- Docker containers enable consistent environment setup and scalable deployments  
- GitHub Actions for automated build, test, and Docker image pushing
- [Architecture Diagram](docs/architecture_diagram.png)  

---

## Data Processing & Database

- Raw NYC taxi/train data cleaned and processed in [data_processing.py](backend/app/data_processing.py)  
- Features computed include trip duration, speed, efficiency, idle time, fare per km  
- Data persisted in normalized SQLite tables: `vendors` and `trips`  
- Full schema defined via [JSON schema](examples/trip_schema.json)
- [ERD Diagram](docs/erd_diagram.png)

---

## API Server & Frontend

- Backend API built with FastAPI, exposing endpoints for trips and vendors  
- Interactive Swagger docs at `/docs` endpoint
- Markdown API docs at [api_docs.md](docs/api_docs.md)
- Frontend UI developed with HTML, CSS, & JavaScript, served on port 3000 during dev  
- Frontend static assets included in backend Docker image for production  

---

## Docker & Deployment

- Backend and frontend Dockerfiles containerize applications  
- Docker Compose config orchestrates services with live code sync in dev  
- GitHub Actions workflow (`.github/workflows/docker-build-push.yml`) automates Docker image build and push  
- Ports configured for backend (8000) and frontend (3000)  

---

## Testing

- Unit and integration tests in `tests/` validate backend data and API endpoint correctness  
- Run all tests with:

  ```
  python -m unittest discover tests
  ```

---

## Environment

This project was developed and tested on:
<!-- ubuntu -->
<a href="https://ubuntu.com/" target="_blank"> <img height="" src="https://img.shields.io/static/v1?label=&message=Ubuntu&color=E95420&logo=Ubuntu&logoColor=E95420&labelColor=2F333A" alt="Suite CRM"></a> <!-- bash --> <a href="https://www.gnu.org/software/bash/" target="_blank"> <img height="" src="https://img.shields.io/static/v1?label=&message=GNU%20Bash&color=4EAA25&logo=GNU%20Bash&logoColor=4EAA25&labelColor=2F333A" alt="terminal"></a> <!-- python--> <a href="https://www.python.org" target="_blank"> <img height="" src="https://img.shields.io/static/v1?label=&message=Python&color=FFD43B&logo=python&logoColor=3776AB&labelColor=2F333A" alt="python"></a> </a> <!-- vim --> <a href="https://www.vim.org/" target="_blank"> <img height="" src="https://img.shields.io/static/v1?label=&message=Vim&color=019733&logo=Vim&logoColor=019733&labelColor=2F333A" alt="Suite CRM"></a> <!-- vs code --> <a href="https://code.visualstudio.com/" target="_blank"> <img height="" src="https://img.shields.io/static/v1?label=&message=Visual%20Studio%20Code&color=5C2D91&logo=Visual%20Studio%20Code&logoColor=5C2D91&labelColor=2F333A" alt="Suite CRM"></a> </a><!-- git --> <a href="https://git-scm.com/" target="_blank"> <img height="" src="https://img.shields.io/static/v1?label=&message=Git&color=F05032&logo=Git&logoColor=F05032&labelColor=2F333A" alt="git distributed version control system"></a> <!-- github --> <a href="https://github.com" target="_blank"> <img height="" src="https://img.shields.io/static/v1?label=&message=GitHub&color=181717&logo=GitHub&logoColor=f2f2f2&labelColor=2F333A" alt="Github"></a>
<!-- Style guidelines -->
Style guidelines:  
- [pycodestyle (version 2.7.*)](https://pypi.org/project/pycodestyle/)  
- [PEP8](https://pep8.org/)  

---

## Team

<details>
<summary>Monica Dhieu -- Backend & ETL Pipeline Lead</summary>
<ul>
<li><a href="https://github.com/m-dhieu">Github</a></li>
<li><a href="https://www.linkedin.com/in/monica-dhieu">LinkedIn</a></li>
<li><a href="mailto:m.dhieu@alustudent.com">e-mail</a></li>
</ul>
</details>

<details>
<summary>Janviere Munezero -- Data Processing, Cleaning & Feature Engineering</summary>
<ul>
<li><a href="https://github.com/Janviere-dev">Github</a></li>
<li><a href="https://www.linkedin.com/in/munezero-janviere-a5375b300">LinkedIn</a></li>
<li><a href="mailto:janviere.munezero@example.com">e-mail</a></li>
</ul>
</details>

<details>
<summary>Thierry Gabin -- Frontend & Data Visualization</summary>
<ul>
<li><a href="https://github.com/tgabin1">Github</a></li>
<li><a href="https://www.linkedin.com/in/#">LinkedIn</a></li>    
<li><a href="mailto:thierry.gabin@example.com">e-mail</a></li>
</ul>
</details>

<details>
<summary>Santhiana Kaze -- Database Design, DevOps & Monitoring</summary>
<ul>
<li><a href="https://github.com/ksanthiana">Github</a></li>
<li><a href="https://www.linkedin.com/in/#">LinkedIn</a></li>    
<li><a href="mailto:santhiana.kaze@example.com">e-mail</a></li>
</ul>
</details>

---

## Setup Instructions

1. Clone the repository:

   ```
   git clone https://github.com/your-org/nyc-train-mobility-app.git
   cd nyc-train-mobility-app
   ```

2. Backend setup:

   ```
   cd backend
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   python data_processing.py
   uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
   ```

3. Frontend setup:

   ```
   cd frontend
   npm install
   npm start
   ```

4. Or run both services with Docker Compose:

   ```
   docker-compose up --build
   ```

## Next Steps After Setup

1. Access the Backend API:

After starting the backend (via uvicorn or Docker Compose), open your browser and navigate to the API documentation:

```
http://localhost:8000/docs
```
Here you can explore, test, and interact with the REST API endpoints for trips and vendors.

2. Use the Frontend Dashboard:

If running the frontend dev server (npm start), open:

```
http://localhost:3000
```

3. If using the backend Docker image for production, open:

```
http://localhost:8000
```
The dashboard interface allows filtering trips by criteria such as date, time, zone, distance, fare, and visualization of trip data and statistics.

4. Try Filtering and Viewing Data:

Use the date picker, sliders, and search boxes to filter trips dynamically. Observe charts and tables updating accordingly.

5. Run Tests:

To verify backend functionality, execute:

```
python -m unittest discover tests
```
View Logs and Errors:

For troubleshooting, check the terminal output of backend and frontend servers. Docker logs can also be viewed with:

```
docker-compose logs -f
```

---

## Contributing

Contributions, issues, and feature requests are welcome!  
Please check the [CONTRIBUTING.md](CONTRIBUTING.md) file for details.

---

## License

MIT License

---

## Contact

For questions or support, reach out to any team member listed above.

---

*Monday, September 29, 2025*
