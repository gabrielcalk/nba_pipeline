FROM python:3.9-slim-bookworm

WORKDIR /app

COPY . .

COPY requirements.txt .

RUN pip install -r requirements.txt

ENV BALL_DONT_LIE_API_KEY=d32782e5-a935-4030-935e-12e1fa32c686
ENV DB_SERVER_NAME=nbabootcamp.cow28mzx354d.us-east-1.rds.amazonaws.com
ENV DB_NAME="nbabootcamp"
ENV DB_USERNAME="gabrielcalk"
ENV DB_PASSWORD="fsdj102923slaof"
ENV PORT=5432
ENV ENV=prod
ENV MODE=increment

ENV AWS_ACCESS_KEY_ID=AKIAWNISAOJNVIXHJHPR
ENV AWS_SECRET_ACCESS_KEY=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
ENV AWS_DEFAULT_REGION=us-east-1

CMD ["python", "-m", "etl.pipelines.balldontlie"]