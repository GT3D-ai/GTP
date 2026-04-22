# Use Google's distroless Node.js image for a small, secure runtime
FROM node:20-alpine AS build
WORKDIR /app

# Install production deps first for better layer caching
COPY package*.json ./
RUN npm ci --omit=dev

# Copy the rest of the app
COPY server.js new-project.js upload.js ./
COPY public ./public

# Create tmp dir for multer (ephemeral, fine on Cloud Run)
RUN mkdir -p /app/tmp

# Cloud Run will set PORT=8080; our server respects process.env.PORT
ENV NODE_ENV=production
EXPOSE 8080

CMD ["node", "server.js"]
