# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY . .

RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONUNBUFFERED=1

# Run worker.py when the container launches
CMD ["python", "./worker.py"]
