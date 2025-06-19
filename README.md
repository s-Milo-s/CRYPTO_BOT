# FastAPI Project

This is a simple FastAPI project that serves as a starting point for building web applications using the FastAPI framework.

## Project Structure

```
fastapi-project
├── app
│   └── app.py          # Main application file
├── Dockerfile           # Dockerfile for containerization
├── requirements.txt     # Python dependencies
└── README.md            # Project documentation
```

## Setup Instructions

1. **Clone the repository:**
   ```
   git clone <repository-url>
   cd fastapi-project
   ```

2. **Create a virtual environment (optional but recommended):**
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install dependencies:**
   ```
   pip install -r requirements.txt
   ```

## Running the Application

To run the FastAPI application, use the following command:

```
uvicorn app.app:app --reload
```

This will start the server at `http://127.0.0.1:8000`. You can access the interactive API documentation at `http://127.0.0.1:8000/docs`.

## Docker Deployment

To build and run the Docker container, use the following commands:

1. **Build the Docker image:**
   ```
   docker build -t fastapi-project .
   ```

2. **Run the Docker container:**
   ```
   docker run -d -p 8000:8000 fastapi-project
   ```

The application will be accessible at `http://localhost:8000`.

## License

This project is licensed under the MIT License.