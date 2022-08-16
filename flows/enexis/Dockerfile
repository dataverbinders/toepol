FROM prefecthq/prefect:latest

RUN pip install --upgrade pip

WORKDIR /opt/prefect

COPY ./src/util/ /opt/prefect/util/
COPY ./requirements.txt .
COPY ./setup.py .

RUN pip install . --no-binary pydantic

COPY ./src/enexis/ /opt/prefect/enexis/

RUN pip install prefect
