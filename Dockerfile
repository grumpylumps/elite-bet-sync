# ---- builder stage: install deps with full Node image ----
FROM node:20-alpine AS builder

WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev

# ---- runtime stage: distroless has near-zero CVE surface ----
FROM gcr.io/distroless/nodejs20-debian12

WORKDIR /app
COPY --from=builder /app/node_modules ./node_modules
COPY . .

EXPOSE 3000

# distroless runs as nonroot (uid 65532) by default
CMD ["src/index.js"]
