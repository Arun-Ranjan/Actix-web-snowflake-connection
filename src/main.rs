use actix_web::{web, App, Error, HttpResponse, HttpServer};
use snowflake_connector_rs::{SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig};
use serde::Deserialize;
use std::{env, fs::File};
use csv::ReaderBuilder;
use actix_cors::Cors;
use serde::Serialize;
use dotenv::dotenv;

#[derive(Deserialize)]
#[derive(Debug)]
struct QueryPayload {
    query: String,
}

#[derive(Deserialize)]
struct CreateTablePayload {
    create_query: String,
    insert_query: String,
}
#[derive(Serialize)]
struct Response {
    message: String,
}

async fn get_snowflake_session() -> Result<snowflake_connector_rs::SnowflakeSession, Error> {
    // Create and return a Snowflake sessionan
    dotenv().ok();
    let user = env::var("SNOWFLAKE_USERNAME").expect("error");
    let password = env::var("SNOWFLAKE_PASSWORD")
    .expect("SNOWFLAKE_PASSWORD is not set");
    let account = env::var("SNOWFLAKE_ACCOUNT").expect("erorr");
    let role = env::var("SNOWFLAKE_ROLE").expect("erorr");
    let warehouse = env::var("SNOWFLAKE_WAREHOUSE").expect("erorr");
    let database = env::var("SNOWFLAKE_DATABASE").expect("erorr");
    let schema = env::var("SNOWFLAKE_SCHEMA").expect("erorr");


    let client = SnowflakeClient::new(
        &user,
        SnowflakeAuthMethod::Password(password),
        SnowflakeClientConfig {
            account,
            role: Some(role),
            warehouse: Some(warehouse),
            database: Some(database),
            schema: Some(schema),
            timeout: Some(std::time::Duration::from_secs(30)),
        },
    )
    .map_err(|e| actix_web::error::ErrorInternalServerError(format!("Client creation failed: {:?}", e)))?;

    let session = client.create_session().await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Session creation failed: {:?}", e))
    })?;
    Ok(session)
}

async fn execute_query(payload: web::Json<QueryPayload>) -> Result<HttpResponse, Error> {
    // Reuse the get_snowflake_session function to get the session
    let session = get_snowflake_session().await?;

    // Execute the query
    match session.query(payload.query.clone()).await {
        Ok(_) => {
            let response = Response {
                message: "Execution Successful!".to_string(),
            };
            Ok(HttpResponse::Ok().json(response))
        },
        Err(e) => {
            let error_response = Response {
                message: format!("Query execution failed: {:?}", e),
            };
            Ok(HttpResponse::InternalServerError().json(error_response))
        },
    }
} 


async fn create_table_in_snowflake(payload: web::Json<CreateTablePayload>) -> Result<HttpResponse, Error> {
    // Reuse the get_snowflake_session function to get the session
    let session = get_snowflake_session().await?;

    // Execute the CREATE TABLE query
    session.query(payload.create_query.clone()).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Create table query execution failed: {:?}", e))
    })?;

    // Insert data from CSV
    let file_path = r"E:\Training\rust\snoflake_connector_rs\connector\iris_data.csv";
    let file = File::open(file_path)?;
    let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(file);

    // Execute the INSERT query for each row in the CSV
    for result in rdr.records() {
        let record = result.expect("Unable to read");
        let row: Vec<String> = record.iter().map(|field| field.to_string()).collect();
        
        // Format the row values and execute the INSERT query
        let values_str = row.iter()
            .map(|field| format!("'{}'", field.replace("'", "''")))
            .collect::<Vec<String>>()
            .join(", ");

        let insert_sql = format!("{} VALUES ({})", payload.insert_query.clone(), values_str);


        let _ = match session.query(insert_sql).await {
            Ok(_) => {
                let response = Response {
                    message: "Table Inserted".to_string(),
                };
                Ok::<HttpResponse, Error>(HttpResponse::Ok().json(response))
            },
            Err(e) => {
                let error_response = Response {
                    message: format!("Query execution failed: {:?}", e),
                };
                Ok(HttpResponse::InternalServerError().json(error_response))
            },
        };
    }
    let response = Response {
        message: "Table created successfully!".to_string(),
    };
    Ok(HttpResponse::Ok()
                    .content_type("application/json")
                    .json(response))
}

#[tokio::main]
async fn main() -> std::io::Result<()> {

    HttpServer::new(move|| {
        App::new()
            // Add the CORS middleware here
            .wrap(
                Cors::default()
                    .allow_any_origin() // Adjust this to your frontend's URL
                    .allow_any_method()
                    .allow_any_header()
            )
            // Define the routes
            .route("/execute", web::post().to(execute_query))
            .route("/create", web::post().to(create_table_in_snowflake))
    })
    .bind("127.0.0.1:8080")?  // Bind to localhost on port 8080
    .run()
    .await
}