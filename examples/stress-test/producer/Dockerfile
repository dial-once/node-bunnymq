FROM dialonce/nodejs:latest

WORKDIR /usr/src/app

COPY . /usr/src/app

RUN npm i --production

EXPOSE 3000

CMD [ "node", "./examples/stress-test/producer/producer.js" ]
