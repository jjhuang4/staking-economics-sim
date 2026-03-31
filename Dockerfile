FROM python:3.12-slim
 
RUN useradd --create-home --shell /bin/bash simuser
 
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /app/output && chown simuser:simuser /app/output
 
COPY *.py ./

ENV MPLBACKEND=Agg
 
USER simuser
 
CMD ["python", "test.py"]