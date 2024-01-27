FROM apache/flink:1.17.2


# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# install PyFlink & dependencies
COPY ./requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt