FROM python:3.13-alpine

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt -v

COPY src/ /app/src

EXPOSE 8080

CMD ["python", "src/app.py"]