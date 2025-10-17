# Use an official Python runtime as a parent image
FROM python:3.13-slim

# Set the working directory in the container
WORKDIR /app

# Copy the pyproject.toml and requirements.txt files to the working directory
COPY pyproject.toml .
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's source code from the host to the container
COPY src/ /app/src

# Install the project in editable mode
RUN pip install -e .

# Copy the start script and make it executable
COPY start.sh .
RUN chmod +x ./start.sh

# Set the entrypoint to the start script
ENTRYPOINT ["./start.sh"]