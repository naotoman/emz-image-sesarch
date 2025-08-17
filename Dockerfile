FROM node:20.18-bookworm

RUN npm install -g esbuild

WORKDIR /root/app

COPY package*.json .
RUN apt update && npm ci --omit=dev

COPY src/ .
RUN esbuild *.ts --outdir=. --target=es2022 --platform=node --format=cjs

CMD ["node", "index.js"]