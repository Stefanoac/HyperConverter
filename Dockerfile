FROM python:3.7

WORKDIR /usr/src/app

RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y python3.7-dev libtinfo5
RUN rm -rf env/ venv/

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY extractapi-py-linux-x86_64-2019-2-1.tar.gz .
RUN tar xvzf extractapi-py-linux-x86_64-2019-2-1.tar.gz
RUN mv hyperextractapi-py-linux-x86_64-release_2019_2.2019.2.1.188.r7a768a19/* .
RUN python setup.py install

COPY . .
RUN chmod -R 777 "/usr/src/app/"
CMD [ "python", "./hyperconverter.py" ]