FROM apache/airflow:3.1.7

# copy requirements files 
COPY requirements.txt /requirements.txt

# install library
RUN pip install --no-cache-dir -r /requirements.txt