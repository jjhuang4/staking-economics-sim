FROM python:3.12-slim
 
RUN useradd --create-home --shell /bin/bash simuser
 
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
COPY *.py ./

ENV MPLBACKEND=Agg
 
USER simuser
 
CMD ["python", "test.py"]