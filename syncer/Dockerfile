FROM python:3-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --upgrade pip && \
	pip install --no-cache-dir -r requirements.txt && \
	rm requirements.txt

COPY ./listen.py .

ENV DB_URL="db"

CMD [ "python", "./listen.py" ]
