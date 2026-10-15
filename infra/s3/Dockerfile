FROM python:3.12-slim
RUN pip install --no-cache-dir 'moto[server,s3]==5.2.4' boto3
EXPOSE 5000
CMD ["moto_server", "-H", "0.0.0.0", "-p", "5000"]
