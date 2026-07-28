FROM oven/bun:latest

WORKDIR /app

COPY package.json .
RUN bun install

COPY resp.ts index.test.ts parity.ts parity-baseline.json ./

CMD ["bun", "test"]
