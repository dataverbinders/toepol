FROM prefecthq/prefect:latest

RUN pip install --upgrade pip

WORKDIR /opt/prefect

COPY ./src/util/ /opt/prefect/util/
COPY ./requirements.txt .
COPY ./setup.py .

RUN pip install . --no-binary pydantic

COPY ./src/bag-extract/ /opt/prefect/bag-extract/
COPY ./src/pyspark/ /opt/prefect/pyspark/

RUN pip install prefect
