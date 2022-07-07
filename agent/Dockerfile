FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:latest

RUN apt update && apt install uuid docker.io -y
RUN pip3 install prefect==0.15.13

COPY ./key.json .
