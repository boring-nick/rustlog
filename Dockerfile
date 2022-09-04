FROM node:latest as frontend
RUN npm install -g pnpm
WORKDIR /src/web
COPY web .
RUN pnpm install
RUN pnpm run build

FROM clux/muslrust:stable AS chef
USER root
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . . 
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --target x86_64-unknown-linux-musl --recipe-path recipe.json
COPY . .
COPY --from=frontend /src/web/build web/
RUN cargo build --release --target x86_64-unknown-linux-musl

FROM alpine AS runtime
RUN addgroup -S rustlog && adduser -S rustlog -G rustlog && mkdir /logs && chown rustlog: /logs
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/rustlog /usr/local/bin/
USER rustlog
CMD ["/usr/local/bin/rustlog"]