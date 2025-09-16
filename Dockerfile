FROM apify/actor-python:3.11

# Cài dependency
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy code
COPY . /app
WORKDIR /app

# Chạy script
CMD ["python", "-u", "main.py"]
