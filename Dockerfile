FROM node:22-alpine
WORKDIR /app
COPY server/package*.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY server/ ./
ENV NODE_ENV=production
ENV NODE_OPTIONS="--max-semi-space-size=64"
CMD ["node", "server.js"]
