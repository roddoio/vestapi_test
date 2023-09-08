extern crate serde;
extern crate serde_json;

use async_graphql::*;
use chrono::{Local};
use std::error::Error;
use std::fmt;
use uuid::Uuid;
use std::env;
use tokio_postgres::{NoTls};
use serde::{Deserialize};

#[derive(Debug)]
struct CustomError {
    message: String,
}

impl CustomError {
    fn new(message: &str) -> Self {
        CustomError {
            message: message.to_string(),
        }
    }
}

impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Custom error: {}", self.message)
    }
}

impl Error for CustomError {}

#[derive(Clone, Debug, SimpleObject)]
struct Stocks {
    id: String,
    stock_symbol: String,
    transaction_type: String,
    stock_units: i32,
    stock_price: f32,
    date_transaction: String
}

#[derive(Clone, Debug, SimpleObject)]
struct ResumeStocks {
    stock_symbol: String,
    profit_lost: String,
    share_held: i64,
    current_value: String,
    current_day_ref_price_min: String,
    current_day_ref_price_max: String
}

#[derive(Clone, Debug, SimpleObject)]
struct HistoricPriceStocks {
    datetime: String,
    price: String
}

#[derive(Clone, Debug, SimpleObject)]
struct NASDAQObj {
    stock_type: String,
    exchange: String,
    last_sale_price : String,
    volume: String,
    net_change: String,
    percentage_change: String,
    min_day_sale_price : String,
    max_day_sale_price : String
}

struct MyContext {
    db_pool: tokio_postgres::Client,
}

impl MyContext {
    fn new(db_pool: tokio_postgres::Client) -> Self {
        MyContext { db_pool }
    }
}

#[derive(Clone, Debug, Deserialize, SimpleObject)]
struct Z {
    dateTime: String,
    value: String,
}

#[derive(Clone, Debug, Deserialize, SimpleObject)]
struct ChartEntry {
    z: Z,
    x: i64,
    y: f64,
}

#[derive(Clone, Debug, Deserialize, SimpleObject)]
struct Data {
    symbol: String,
    company: String,
    timeAsOf: String,
    isNasdaq100: bool,
    lastSalePrice: String,
    netChange: String,
    percentageChange: String,
    deltaIndicator: String,
    previousClose: String,
    volume: String,
    chart: Vec<ChartEntry>,
    events: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, SimpleObject)]
struct Status {
    rCode: u32,
}

#[derive(Clone, Debug, Deserialize, SimpleObject)]
struct Response {
    data: Data,
    message: Option<serde_json::Value>,
    status: Status,
}

#[derive(Default)]
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn stocks_by_user(&self, id: String) -> Result<Vec<Stocks>, tokio_postgres::Error> {
        let result = get_stocks_by_user(id).await;
        result
    }

    async fn resume_stocks_by_user(&self, id: String) -> Result<Vec<ResumeStocks>, tokio_postgres::Error> {
        let result = get_resume_stocks_by_user(id).await;
        result
    }

    async fn historic_price_by_stock(&self, stock: String) -> Result<Vec<HistoricPriceStocks>, Box<dyn Error  + Send + Sync>> {
        let result = get_historic_price_by_stock(stock).await;
        result
    }

    async fn test(&self) -> String {
        "Hello, friend!".to_string()
    }
}

#[derive(Default)]
pub struct VestTransactions;

#[Object]
impl VestTransactions {
    async fn buy_symbol(&self, _id_user: String, _stock_symbol: String, _stock_units: i32) -> String {
        let result = save_to_kf(_id_user, String::from("BY"), _stock_symbol, _stock_units).await;
        match result
        {
            Ok(result) => format!("Resultado: {}", result),
            Err(error) => format!("Error: {}", error),
        }
    }

    async fn sell_symbol(&self, _id_user: String, _stock_symbol: String, _stock_units: i32) -> String {
        let result = save_to_kf(_id_user, String::from("SL"), _stock_symbol, _stock_units).await;
        match result
        {
            Ok(result) => format!("Resultado: {}", result),
            Err(error) => format!("Error: {}", error),
        }
    }
}

async fn save_to_kf(_id_user: String, _transaction_type: String, _stock_symbol: String, _stock_units: i32) -> Result<String, Box<dyn Error>>
{
    static APP_USER_AGENT: &str = concat!(
        env!("CARGO_PKG_NAME"),
        "/",
        env!("CARGO_PKG_VERSION"),
    );
    
    let client = reqwest::Client::builder()
        .user_agent(APP_USER_AGENT)
        .build()?;


    let url = format!("https://api.nasdaq.com/api/quote/{}/info?assetclass=stocks", _stock_symbol);

    let response = client.get(&url).send().await?;
    
    if response.status().is_success() {

        let body = response.text().await?;
        let parsed = json::parse(&body).unwrap();
        let code_nasdaq = &parsed["status"]["rCode"];
        if code_nasdaq == 200
        {
            //Crea el mensaje en Kafka
            let _stock_type = &parsed["data"]["stockType"].to_string();
            let _exchange = &parsed["data"]["exchange"].to_string();
            let _last_sale_price = &parsed["data"]["primaryData"]["lastSalePrice"].to_string();
            let _volume = &parsed["data"]["primaryData"]["volume"].to_string();
            let _net_change = &parsed["data"]["primaryData"]["netChange"].to_string();
            let _percentage_change = &parsed["data"]["primaryData"]["percentageChange"].to_string();
            
            let _gid = Uuid::new_v4();

            let client_kf = reqwest::Client::new();
            let url_k = env::var("URL_KF_ORDERS").expect("$URL_KF_ORDERS is not set");
            let api_key_kf = env::var("API_KEY_KF_CLUSTER").expect("$API_KEY_KF_CLUSTER is not set");
            let api_sec_kf = env::var("API_SECRET_KF_CLUSTER").expect("$API_SECRET_KF_CLUSTER is not set");

            let _body = format!(r#"{{
                "key": {{
                    "type": "JSON",
                    "data": {{
                        "id": "{}"
                    }}
                }},
                "value": {{
                    "type": "JSON",
                    "data": {{
                        "id_user": "{}",
                        "transaction_type": "{}",
                        "stock_symbol": "{}",
                        "stock_units": {},
                        "stock_price": {},
                        "date_transaction": "{}"
                    }}
                }}
            }}"#,_gid, _id_user, _transaction_type, _stock_symbol, _stock_units, _last_sale_price.replace("$", ""), Local::now());

            println!("Kafka details {}",_body);

            let response_kf = client_kf.post(url_k)
            .header("Content-Type", "application/json")
            .header("Accept", "*/*")
            .basic_auth(api_key_kf, Some(api_sec_kf))
            .body(_body.to_owned())
            .send().await?; 

            if response_kf.status().is_success() {
                let body_kf = response_kf.text().await?;
                let _parsed_kf = json::parse(&body_kf).unwrap();
            }
            else {
                let msg_resp = &format!("KAFKA API Bad response: {}", response_kf.status());
                println!("{}",msg_resp);
                return Err(Box::new(CustomError::new(&msg_resp)));
            }

            return Ok(format!("{} {} stocks {} successfully by {}",_stock_units, _stock_symbol, _transaction_type,_last_sale_price).to_string());
        }
        else{
            let msg_resp = &format!("NASDAQ API Wrong code:{}", parsed["status"]["bCodeMessage"][0]["errorMessage"]);
            println!("{}",msg_resp);
            return Err(Box::new(CustomError::new(&msg_resp)));
        }
    }
    else {
        let msg_resp = &format!("NASDAQ API Bad response: {}", response.status());
        println!("{}",msg_resp);
        return Err(Box::new(CustomError::new(&msg_resp)));
    }
}

async fn get_stocks_by_user(id: String) -> Result<Vec<Stocks>, tokio_postgres::Error> {

    let db_url = env::var("DB_VEST_CON").expect("$DB_VEST_CON is not set");
    let (db_client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;
    tokio::spawn(connection);

    let context = MyContext::new(db_client);

    let stmt = context.db_pool.prepare("select id, stock_symbol, transaction_type,stock_units,stock_price,TO_CHAR(date_transaction, 'YYYY/MM/DD HH24:MM:SS') from orders where id_user = $1").await?;
    let rows = context.db_pool.query(&stmt, &[&id]).await?;
    
    let mut stocks_list = Vec::new();

    for row in &rows {
        stocks_list.push(Stocks {
            id: row.get(0),
            stock_symbol: row.get(1),
            transaction_type: row.get(2),
            stock_units: row.get(3),
            stock_price: row.get(4),
            date_transaction: row.get(5),
        });
    }

    Ok(stocks_list)
}

async fn get_resume_stocks_by_user(id: String) -> Result<Vec<ResumeStocks>, tokio_postgres::Error> {

    let db_url = env::var("DB_VEST_CON").expect("$DB_VEST_CON is not set");
    let (db_client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;
    tokio::spawn(connection);

    let context = MyContext::new(db_client);

    let stmt = context.db_pool.prepare("
    select q.*, (select min(u.stock_price) from orders u where u.stock_symbol = q.stock_symbol and u.transaction_type = 'BY' and u.date_transaction = q.first_date) as first_price,
            (select max(u.stock_price) from orders u where u.stock_symbol = q.stock_symbol and u.transaction_type = 'BY' and u.date_transaction = q.recent_date) as recent_price 
            from (
    select x.*,  
            (select min(u.date_transaction) from orders u where u.stock_symbol = x.stock_symbol and u.transaction_type = 'BY') as first_date,
            (select max(u.date_transaction) from orders u where u.stock_symbol = x.stock_symbol and u.transaction_type = 'BY') as recent_date
    from (
    select distinct a.stock_symbol,
            sum(a.stock_units) as num_Stocks
    from orders a where a.id_user =$1 group by a.stock_symbol) as x) as q").await?;
    let rows = context.db_pool.query(&stmt, &[&id]).await?;
    
    let mut stocks_list = Vec::new();

    for row in &rows {
        let res = get_val_stock(row.get(0)).await;
        match res {
            Ok(res) => { 
                let _min_val: f32 = row.get(4);
                let _curr_val: f32 = res.last_sale_price.parse::<f32>().unwrap();
                let _num_stocks: i64 = row.get(1);
                let profit = format!("{} %", (((_curr_val / _min_val) - 1.0) * 100.0).to_string());
                let total_val_stocks = _curr_val * _num_stocks as f32;

                stocks_list.push(ResumeStocks {
                    stock_symbol: row.get(0),
                    profit_lost: profit,
                    share_held: row.get(1),
                    current_value: format!("{}", total_val_stocks),
                    current_day_ref_price_min: res.min_day_sale_price,
                    current_day_ref_price_max: res.max_day_sale_price,
        });
            },
            Err(_e) => {
                println!("Error: {}",_e);
            },
        }  
    }

    Ok(stocks_list)
}

async fn get_historic_price_by_stock(stock: String) -> Result<Vec<HistoricPriceStocks>, Box<dyn Error  + Send + Sync>> {

    let mut stocks_list = Vec::new();

    static APP_USER_AGENT: &str = concat!(
        env!("CARGO_PKG_NAME"),
        "/",
        env!("CARGO_PKG_VERSION"),
    );
    
    let client = reqwest::Client::builder()
        .user_agent(APP_USER_AGENT)
        .build()?;

    let url = format!("https://api.nasdaq.com/api/quote/{}/chart?assetclass=stocks", stock);

    let response = client.get(&url).send().await?;
    
    if response.status().is_success() {

        let body = response.text().await?;

        let response_n: Response = serde_json::from_str(&body).unwrap();
  
        if response_n.status.rCode == 200
        {
            for entry in response_n.data.chart {

                if entry.z.dateTime.contains(":00")
                {
                    stocks_list.push(HistoricPriceStocks {
                        datetime: entry.z.dateTime,
                        price: entry.z.value
                    });
                }
            }
        }
        else{
            let msg_resp = &format!("NASDAQ API Wrong code:{}", response_n.status.rCode);
            println!("{}",msg_resp);
            return Err(Box::new(CustomError::new(&msg_resp)));
        }
    }
    else {
        let msg_resp = &format!("NASDAQ API Bad response: {}", response.status());
        println!("{}",msg_resp);
        return Err(Box::new(CustomError::new(&msg_resp)));
    }

    Ok(stocks_list)
}


async fn get_val_stock(_stock_symbol: String) -> Result<NASDAQObj, Box<dyn Error>>
{
    static APP_USER_AGENT: &str = concat!(
        env!("CARGO_PKG_NAME"),
        "/",
        env!("CARGO_PKG_VERSION"),
    );
    
    let client = reqwest::Client::builder()
        .user_agent(APP_USER_AGENT)
        .build()?;

    let url = format!("https://api.nasdaq.com/api/quote/{}/info?assetclass=stocks", _stock_symbol);

    let response = client.get(&url).send().await?;
    
    if response.status().is_success() {

        let body = response.text().await?;
        let parsed = json::parse(&body).unwrap();
        let code_nasdaq = &parsed["status"]["rCode"];
        if code_nasdaq == 200
        {

            let v1 = parsed["data"]["keyStats"]["dayrange"]["value"].to_string();
            let vals_day_range: Vec<&str> = v1.split("-").collect();

            Ok(NASDAQObj {
                stock_type: parsed["data"]["stockType"].to_string(),
                exchange: parsed["data"]["exchange"].to_string(),
                last_sale_price: parsed["data"]["primaryData"]["lastSalePrice"].to_string().replace("$", ""),
                volume: parsed["data"]["primaryData"]["volume"].to_string(),
                net_change: parsed["data"]["primaryData"]["netChange"].to_string(),
                percentage_change: parsed["data"]["primaryData"]["percentageChange"].to_string(),
                min_day_sale_price : String::from(vals_day_range[0].replace(" ", "")),
                max_day_sale_price : String::from(vals_day_range[1].replace(" ", "")),
            })
        }
        else{
            let msg_resp = &format!("NASDAQ API Wrong code:{}", parsed["status"]["bCodeMessage"][0]["errorMessage"]);
            println!("{}",msg_resp);
            return Err(Box::new(CustomError::new(&msg_resp)));
        }
    }
    else {
        let msg_resp = &format!("NASDAQ API Bad response: {}", response.status());
        println!("{}",msg_resp);
        return Err(Box::new(CustomError::new(&msg_resp)));
    }
}

pub type MySchema = Schema<QueryRoot, VestTransactions, EmptySubscription>;