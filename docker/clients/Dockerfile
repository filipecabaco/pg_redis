FROM node:22-alpine

WORKDIR /app

COPY package.json .
RUN npm install --no-audit --no-fund

COPY run_tests.mjs .

CMD ["node", "run_tests.mjs"]
