FROM oven/bun:latest

WORKDIR /app

COPY package.json .
RUN bun install

COPY index.test.ts .

CMD ["bun", "test"]
