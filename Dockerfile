FROM python:3.12
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD [ "startup.sh" ]
