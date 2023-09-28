use actix_web::{get, App, HttpRequest, HttpResponse, HttpServer, web, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Input {
    age: i32,
    sex: String,
}

#[derive(Debug, Serialize)]
enum Prediction {
    Boy,
    Man,
    Girl,
    Woman,
}

#[get("/predict")]
async fn predict_model(req: web::Json<Input>) -> Result<HttpResponse> {
    let age = req.age;
    let sex = req.sex.to_uppercase();

    let prediction = match (age, &sex[..]) {
        (age, "MALE") if age <= 17 => Prediction::Boy,
        (age, "MALE") if age >= 18 => Prediction::Man,
        (age, "FEMALE") if age <= 17 => Prediction::Girl,
        (age, "FEMALE") if age >= 18 => Prediction::Woman,
        _ => {
            return Ok(HttpResponse::BadRequest().json("Please provide valid parameters for age and sex"));
        }
    };

    Ok(HttpResponse::Ok().json(prediction))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .service(predict_model)
    })
    .bind("127.0.0.1:8008")?
    .run()
    .await
}
