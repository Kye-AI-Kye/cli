# Use a lightweight, modern Python base image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the agent code into the container
COPY godprotocol_agent.py .

# The command to run when the container starts
# This launches the agent in the persistent "worker" mode
CMD ["python", "godprotocol_agent.py", "worker"]
