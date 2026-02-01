FROM python:3.10

WORKDIR /app

COPY ./scripts/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["tail", "-f", "/dev/null"]
