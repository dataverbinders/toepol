FROM prefecthq/prefect:latest

RUN pip install --upgrade pip

WORKDIR /opt/prefect

COPY ./src/util/ ./util/
COPY ./requirements.txt .
COPY ./setup.py .

RUN pip install . --no-binary pydantic

COPY ./src/bag-extract/ ./bag-extract/
COPY ./src/pyspark/ ./pyspark/

RUN pip install prefect
