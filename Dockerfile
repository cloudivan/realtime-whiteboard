FROM node:22-alpine
WORKDIR /app
COPY server/package*.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY server/ ./
ENV NODE_ENV=production
CMD ["node", "server.js"]
