FROM node:20-alpine
WORKDIR /usr/src/app
COPY package.json package-lock.json ./
RUN npm install --production
COPY tsconfig.json ./
COPY src ./src
COPY src/protos ./src/protos
RUN npx tsc
CMD ["node", "dist/index.js"]
