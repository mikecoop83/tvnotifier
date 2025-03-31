use chrono::{DateTime, Days, Local};
use lettre::{
    message::SinglePart, transport::smtp::authentication::Credentials, Message, SmtpTransport,
    Transport,
};
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::fmt::Formatter;
use std::fs;
use tokio_postgres::{self};

const DATE_TIME_FORMAT: &str = "%a. %b. %d %l:%M %p";
const DATE_FORMAT: &str = "%a. %b. %d";
const FUTURE_DAY_LIMIT: u64 = 7;

#[derive(Serialize, Deserialize)]
struct Config {
    pg_connection_string: String,
    smtp_server: String,
    smtp_host: String,
    smtp_user: String,
    smtp_password: String,
    from_email: String,
    site_url: String,
    rapid_api_key: String,
    movie_platforms: Vec<String>,
}

#[derive(Debug)]
struct Show {
    id: i32,
    name: String,
    episode_name: String,
    show_time: DateTime<chrono::Local>,
}

impl fmt::Display for Show {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {} ({})",
            self.show_time.format(DATE_TIME_FORMAT),
            self.name,
            self.episode_name
        )
    }
}

impl Show {
    fn html(&self) -> String {
        format!(
            "{}: <a href=\"https://www.tvmaze.com/shows/{}\">{}</a> ({})",
            self.show_time.format(DATE_TIME_FORMAT),
            self.id,
            self.name,
            self.episode_name,
        )
    }
}

#[tokio::main]
async fn main() {
    let mut config_file = String::new();
    let mut no_mail = false;
    let mut debug = false;
    let _: Vec<String> = go_flag::parse(|flags| {
        flags.add_flag("config", &mut config_file);
        flags.add_flag("nomail", &mut no_mail);
        flags.add_flag("debug", &mut debug);
    });

    let config_content = fs::read_to_string(config_file).expect("config file not found");
    let config = serde_json::from_str::<Config>(&config_content).expect("invalid config");
    let show_ids = get_ids(IdType::Show, &config).await.unwrap();
    let shows = get_shows_parallel(show_ids)
        .await
        .expect("failed getting episode details");

    let movie_ids = get_ids(IdType::Movie, &config).await.unwrap();
    let subscribed_movie_platforms: HashSet<String> =
        config.movie_platforms.iter().cloned().collect();
    let mut movie_to_platforms: std::collections::HashMap<String, HashSet<String>> =
        std::collections::HashMap::new();
    for movie_id in movie_ids {
        let movie = get_streaming_platforms(&config.rapid_api_key, movie_id)
            .await
            .expect("failed to get movie platforms");
        let platforms = movie.platforms;
        let title = movie.title;
        let platforms_set: HashSet<String> = platforms.into_iter().collect();
        let intersection: HashSet<String> = subscribed_movie_platforms
            .intersection(&platforms_set)
            .cloned()
            .collect();
        if intersection.len() > 0 {
            movie_to_platforms.insert(title, intersection);
        }
    }

    if no_mail {
        shows.iter().for_each(|show| println!("{show}"));
        movie_to_platforms.iter().for_each(|(movie_id, platforms)| {
            println!(
                "{movie_id} available on {platforms:?}",
                movie_id = movie_id,
                platforms = platforms
            )
        });
        return ();
    }
    let subscriptions = get_subscriptions(&config)
        .await
        .expect("failed to get subscriptions");

    send_email(&shows, &config, subscriptions).expect("couldn't send the email");
}

fn send_email(
    shows: &Vec<Show>,
    config: &Config,
    subscriptions: Vec<String>,
) -> Result<(), Box<dyn Error>> {
    let today = Local::now().date_naive();
    let future_date_limit = today.checked_add_days(Days::new(FUTURE_DAY_LIMIT)).unwrap();
    let today = Local::now().date_naive();
    let mut today_shows = vec![];
    let mut future_shows = vec![];
    for show in shows {
        if show.show_time.date_naive() > future_date_limit {
            break;
        }
        if show.show_time.date_naive() == today {
            today_shows.push(show);
        } else {
            future_shows.push(show);
        }
    }
    let mut message = "<pre><b>Today's shows:<br />".to_owned();
    if today_shows.len() > 0 {
        for show in today_shows {
            message.push_str(show.html().as_str());
            message.push_str("<br />");
        }
    } else {
        message.push_str("</i>Nothing airing today.</i>");
    }
    message.push_str("</b><br /><br />");

    if future_shows.len() > 0 {
        message.push_str("Future shows:<br />");
        for show in future_shows {
            message.push_str(show.html().as_str());
            message.push_str("<br />");
        }
    }
    message.push_str(
        format!(
            "<br /><br />Manage subscriptions on <a href=\"{}\">TV Notifier UI</a>",
            config.site_url
        )
        .as_ref(),
    );
    message.push_str("</pre>");

    let mut builder = Message::builder().from(config.from_email.parse().unwrap());

    for sub in subscriptions {
        builder = builder.to(sub.parse().unwrap());
    }

    let email = builder
        .subject(format!("Upcoming shows for {}", today.format(DATE_FORMAT)))
        .singlepart(SinglePart::html(message))
        .unwrap();

    let creds = Credentials::new(
        config.smtp_user.to_string(),
        config.smtp_password.to_string(),
    );

    // Open a remote connection to gmail
    let mailer = SmtpTransport::relay(&config.smtp_host)
        .unwrap()
        .credentials(creds)
        .build();

    if let Err(e) = mailer.send(&email) {
        return Err(Box::new(e));
    }
    Ok(())
}

enum IdType {
    Show,
    Movie,
}
async fn get_ids(id_type: IdType, config: &Config) -> Result<Vec<i32>, Box<dyn Error>> {
    let pg_connection_string = &config.pg_connection_string;
    let builder = SslConnector::builder(SslMethod::tls())?;
    let connector = MakeTlsConnector::new(builder.build());

    let (client, connection) = tokio_postgres::connect(&pg_connection_string, connector).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move { connection.await });

    let id_type_str = match id_type {
        IdType::Show => "shows",
        IdType::Movie => "movies",
    };

    let ids: Vec<i32> = client
        .query(format!("SELECT id FROM {id_type_str}").as_str(), &[])
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    Ok(ids)
}

async fn get_subscriptions(config: &Config) -> Result<Vec<String>, Box<dyn Error>> {
    let pg_connection_string = &config.pg_connection_string;
    let builder = SslConnector::builder(SslMethod::tls())?;
    let connector = MakeTlsConnector::new(builder.build());

    let (client, connection) = tokio_postgres::connect(&pg_connection_string, connector).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move { connection.await });

    let subscriptions: Vec<String> = client
        .query("select email from users where email is not null", &[])
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    Ok(subscriptions)
}

fn parse_show(show_id: i32, show_name: &str, episode_details: &Map<String, Value>) -> Show {
    let episode_name = episode_details["name"].as_str().unwrap_or_default();
    let airstamp = episode_details["airstamp"].as_str().unwrap_or_default();
    let show_time = DateTime::parse_from_rfc3339(airstamp).unwrap_or_default();
    Show {
        id: show_id,
        name: show_name.to_owned(),
        episode_name: episode_name.to_owned(),
        show_time: show_time.with_timezone(&chrono::Local),
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Root {
    result: MovieResult,
}

#[derive(Serialize, Deserialize, Debug)]
struct MovieResult {
    #[serde(rename = "streamingInfo")]
    streaming_info: StreamingInfo,
    title: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct StreamingInfo {
    us: Vec<Service>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Service {
    service: String,
    #[serde(rename = "streamingType")]
    streaming_type: String,
    addon: Option<String>,
}

struct Movie {
    title: String,
    platforms: Vec<String>,
}

async fn get_streaming_platforms(api_key: &str, movie_id: i32) -> Result<Movie, Box<dyn Error>> {
    let url = format!("https://streaming-availability.p.rapidapi.com/get?output_language=en&tmdb_id=movie/{movie_id}");
    // add a header for the api key

    let mut headers = HeaderMap::new();
    headers.insert("X-RapidAPI-Key", HeaderValue::from_str(api_key)?);
    headers.insert(
        "X-RapidAPI-Host",
        HeaderValue::from_static("streaming-availability.p.rapidapi.com"),
    );

    let client = reqwest::Client::new();

    let response = client.get(url).headers(headers).send().await?;

    if let Err(err) = response.error_for_status_ref() {
        return Err(Box::new(err));
    }
    let body = response.text().await?;

    let root: Root = serde_json::from_str(&body)?;

    let title = root.result.title;

    let subscription_services: Vec<_> = root
        .result
        .streaming_info
        .us
        .iter()
        .filter(|service| {
            service.streaming_type == "subscription" || service.streaming_type == "addon"
        })
        .map(|service| {
            service
                .addon
                .clone()
                .unwrap_or_else(|| service.service.clone())
        })
        .collect();

    Ok(Movie {
        title,
        platforms: subscription_services,
    })
}

async fn get_next_episode(show_id: i32) -> Result<Option<Show>, Box<dyn Error>> {
    let url = format!(
        "https://api.tvmaze.com/shows/{show_id}?embed[]=nextepisode&embed[]=previousepisode"
    );
    let response = reqwest::get(url).await?;
    if let Err(err) = response.error_for_status_ref() {
        return Err(Box::new(err));
    }
    let body = response.text().await?;
    let show: Value = serde_json::from_str(&body)?;
    let show_name = show["name"].as_str().ok_or("show name not found")?;
    let embedded = &show["_embedded"];
    if !embedded.is_object() {
        return Ok(None);
    }
    let prev_episode = &embedded["previousepisode"];
    if prev_episode.is_object() {
        let prev_show = parse_show(
            show_id,
            show_name,
            prev_episode
                .as_object()
                .expect("previous epsiode not an object"),
        );
        if prev_show.show_time.date_naive() == Local::now().date_naive() {
            return Ok(Some(prev_show));
        }
    }
    let next_episode = &embedded["nextepisode"];
    if !next_episode.is_object() {
        return Ok(None);
    }
    let next_show = parse_show(
        show_id,
        show_name,
        next_episode
            .as_object()
            .expect("next epsiode not an object"),
    );
    Ok(Some(next_show))
}

async fn get_shows_parallel(show_ids: Vec<i32>) -> Result<Vec<Show>, Box<dyn Error>> {
    let mut show_handles = vec![];
    for show_id in show_ids {
        show_handles.push(tokio::spawn(async move {
            let next_episode = get_next_episode(show_id).await;
            match next_episode {
                Ok(show) => Ok(show),
                Err(err) => Err(err.to_string()),
            }
        }))
    }
    let mut shows = vec![];
    for show_handle in show_handles {
        if let Some(show) = show_handle.await?.unwrap() {
            shows.push(show)
        }
    }
    shows.sort_by(|a, b| a.show_time.cmp(&b.show_time));
    Ok(shows)
}
