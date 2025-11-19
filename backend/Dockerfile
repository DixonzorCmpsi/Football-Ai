# 1. Start from a lightweight, official Python image
FROM python:3.11-slim

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy just the requirements file and install dependencies
# This is a best practice. Docker will cache this layer,
# so it won't reinstall all your libraries every time you
# change one line of your server code.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy all your application code and data into the container
# This copies your server and feature generator
COPY applications/ ./applications/
COPY dataPrep/feature_generator_timeseries.py ./dataPrep/feature_generator_timeseries.py

# This copies your live 2025 RAG data
COPY rag_data/ ./rag_data/

# This copies your 4 trained models and 4 feature lists
COPY model_training/models/ ./model_training/models/

# 5. Expose the port your FastAPI server runs on
EXPOSE 8000

# 6. The command to run your server
# We use 0.0.0.0 to make it accessible from outside the container.
CMD ["uvicorn", "applications.server:app", "--host", "0.0.0.0", "--port", "8000"]