FROM ubuntu:22.04

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update -y && \
    apt install -y python3 python3-pip python3-full openjdk-17-jdk curl unixodbc unixodbc-dev lsb-release netcat && \
    apt clean && \
    mkdir /pyspark

COPY ./scripts/install_odbc.sh /pyspark/install_odbc.sh

WORKDIR /pyspark

RUN chmod +x ./install_odbc.sh && ./install_odbc.sh

RUN pip install pyspark==3.5.3 pyodbc matplotlib pandas jinja2

CMD ["/bin/bash", "-c", "while ! nc -z db 1433; do sleep 1; done; python3 /pyspark/scripts/dw_constructor.py && python3 /pyspark/scripts/report_constructor.py"]
