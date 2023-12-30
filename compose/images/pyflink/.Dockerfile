
FROM apache/flink:1.17.1
# The liblzma-dev is important to apache beam runtime
RUN apt-get update -y && \
apt-get install -y build-essential liblzma-dev libssl-dev zlib1g-dev libbz2-dev libffi-dev && \
wget https://www.python.org/ftp/python/3.9.7/Python-3.9.7.tgz && \
tar -xvf Python-3.9.7.tgz && \
cd Python-3.9.7 && \
./configure --without-tests --enable-shared && \
make -j6 && \
make install && \
ldconfig /usr/local/lib && \
cd .. && rm -f Python-3.9.7.tgz && rm -rf Python-3.9.7 && \
ln -s /usr/local/bin/python3 /usr/local/bin/python && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

# install PyFlink & dependencies
COPY ./requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt