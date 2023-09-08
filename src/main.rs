use std::convert::Infallible;

use async_graphql::{http::GraphiQLSource, EmptySubscription, Schema};
use async_graphql_warp::{GraphQLBadRequest, GraphQLResponse};
use http::StatusCode;
use warp::{http::Response as HttpResponse, Filter, Rejection, Reply};

mod schema;

type Result<T> = std::result::Result<T, Rejection>;

#[tokio::main]
async fn main() {
    
    let schema = schema::MySchema::new(schema::Consultas, schema::VestTransactions, EmptySubscription);

    let graphql_post = async_graphql_warp::graphql(schema).and_then(
        |(schema, request): (
            Schema<schema::Consultas, schema::VestTransactions, EmptySubscription>,
            async_graphql::Request,
        )| async move {
            Ok::<_, Infallible>(GraphQLResponse::from(schema.execute(request).await))
        },
    );

    let graphiql = warp::path::end().and(warp::get()).map(|| {
        HttpResponse::builder()
            .header("content-type", "text/html")
            .body(GraphiQLSource::build().endpoint("/").finish())
    });

    let health_route = warp::path!("health").and_then(health_handler);

    let routes = graphiql
        .or(graphql_post)
        .or(health_route)
        .with(warp::cors().allow_any_origin())
        .recover(|err: Rejection| async move {
            if let Some(GraphQLBadRequest(err)) = err.find() {
                return Ok::<_, Infallible>(warp::reply::with_status(
                    err.to_string(),
                    StatusCode::BAD_REQUEST,
                ));
            }

            Ok(warp::reply::with_status(
                "INTERNAL_SERVER_ERROR".to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            ))
        });
    
    println!("Service Started on port 8000");
    //warp::serve(routes).run(([127, 0, 0, 1], 8000)).await;
    warp::serve(routes).run(([0, 0, 0, 0], 8000)).await;
}

async fn health_handler() -> Result<impl Reply> {
    Ok("OK")
}