FROM node:18-alpine AS builder
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm config set strict-ssl=false
RUN npm install
COPY . ./
# CMD ["npm", "run", "dev"]
RUN npm run build

FROM nginx
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf