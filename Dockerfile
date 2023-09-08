FROM rust:bookworm as builder

ENV URL_KF_ORDERS=https://pkc-lgk0v.us-west1.gcp.confluent.cloud/kafka/v3/clusters/lkc-pkmpzy/topics/orders/records
ENV API_KEY_KF_CLUSTER=3UG7LZEFZFIPCEAT
ENV API_SECRET_KF_CLUSTER=P2YrMqL254KNj/hGpJ+RCj1CQqEAWrb3n+zx+moX8wmO5Yenokq/ii5d8shl9RGc
ENV DB_VEST_CON=postgresql://postgres:postgres@localhost/vest
EXPOSE 8000

RUN USER=root cargo new --bin api_be
WORKDIR ./api_be
COPY ./Cargo.toml ./Cargo.toml
RUN cargo build --release
RUN rm src/*.rs

ADD . ./

RUN rm ./target/release/deps/api_be*
RUN cargo build --release


FROM debian:bookworm-slim

ENV URL_KF_ORDERS=https://pkc-lgk0v.us-west1.gcp.confluent.cloud/kafka/v3/clusters/lkc-pkmpzy/topics/orders/records
ENV API_KEY_KF_CLUSTER=3UG7LZEFZFIPCEAT
ENV API_SECRET_KF_CLUSTER=P2YrMqL254KNj/hGpJ+RCj1CQqEAWrb3n+zx+moX8wmO5Yenokq/ii5d8shl9RGc
ENV DB_VEST_CON=postgresql://postgres:postgres@localhost/vest

ARG APP=/usr/src/app

RUN apt-get update \
    && apt-get install -y ca-certificates tzdata \
    && apt-get install -y openssl \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 8000

ENV TZ=Etc/UTC \
    APP_USER=appuser

RUN groupadd $APP_USER \
    && useradd -g $APP_USER $APP_USER \
    && mkdir -p ${APP}

COPY --from=builder /api_be/target/release/api_be ${APP}/api_be

RUN chown -R $APP_USER:$APP_USER ${APP}

USER $APP_USER
WORKDIR ${APP}

CMD ["./api_be"]