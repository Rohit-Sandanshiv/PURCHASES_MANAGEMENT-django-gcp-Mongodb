# Use official Python image as base
FROM python:3.9

# Set working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Django project files
COPY . .

# Expose port 8080 for Cloud Run
EXPOSE 8080

# Start the Django app with gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "django_api.wsgi:application"]
