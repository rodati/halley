FROM node:10

RUN npm i -g pm2

COPY . .
RUN npm i

ENTRYPOINT node index.js
