FROM python:3.9-slim-bookworm

WORKDIR /app

COPY . .

COPY requirements.txt .

RUN pip install -r requirements.txt

<<<<<<< HEAD
=======
ENV BALL_DONT_LIE_API_KEY=d32782e5-a935-4030-935e-12e1fa32c686
ENV DB_SERVER_NAME=nbabootcamp.cow28mzx354d.us-east-1.rds.amazonaws.com
ENV DB_NAME="nbabootcamp"
ENV DB_USERNAME="gabrielcalk"
ENV DB_PASSWORD="fsdj102923slaof"
ENV PORT=5432

>>>>>>> c721fb6574b5f2d25107a5c8fc5a511cb3328eef
CMD ["python", "-m", "etl.pipelines.balldontlie"]