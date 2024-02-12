FROM flink:1.17.2


# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# Re-install jre&jdk
RUN apt-get update -y && \
apt-get install -y openjdk-11-jre openjdk-11-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64/

# install PyFlink & dependencies
COPY ./requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt