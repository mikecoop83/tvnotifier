use chrono::{DateTime, Days, Local};
use lettre::{
    message::SinglePart, transport::smtp::authentication::Credentials, Message, SmtpTransport,
    Transport,
};
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::error::Error;
use std::fmt;
use std::fmt::Formatter;
use std::fs;
use tokio_postgres::{self};

const FUTURE_LIMIT_DAYS: u64 = 7;
const DATE_TIME_FORMAT: &str = "%a. %b. %d %l:%M %p";
const DATE_FORMAT: &str = "%a. %b. %d";

#[derive(Serialize, Deserialize)]
struct Config {
    pg_connection_string: String,
    smtp_server: String,
    smtp_host: String,
    smtp_user: String,
    smtp_password: String,
    from_email: String,
    to_emails: Vec<String>,
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
            self.episode_name
        )
    }
}

#[tokio::main]
async fn main() {
    let mut config_file = "".to_owned();
    let mut no_mail = false;
    let _: Vec<String> = go_flag::parse(|flags| {
        flags.add_flag("config", &mut config_file);
        flags.add_flag("nomail", &mut no_mail);
    });

    let config_content = fs::read_to_string(config_file).expect("config file not found");
    let config = serde_json::from_str::<Config>(&config_content).expect("invalid config");
    let show_ids = get_show_ids(&config).await.unwrap();
    let shows = get_shows_parallel(show_ids)
        .await
        .expect("failed getting episode details");
    if no_mail {
        shows.iter().for_each(|show| println!("{show}"));
        return ();
    }
    send_email(&shows, &config).expect("couldn't send the email");
}

fn send_email(shows: &Vec<Show>, config: &Config) -> Result<(), Box<dyn Error>> {
    let today = Local::now().date_naive();
    let mut message = "<pre>".to_owned();
    for show in shows {
        let mut before = "";
        let mut after = "";
        if show.show_time.date_naive() == today {
            before = "<b>";
            after = "</b>";
        }
        message.push_str(before);
        message.push_str(show.html().as_str());
        message.push_str(after);
        message.push('\n');
    }
    message.push_str("</pre>");
    message.push_str("<br /><br /><a href=\"https://tvnotifier-ui.vercel.app\">TVNotifier UI</a>");

    let mut builder = Message::builder().from(config.from_email.parse().unwrap());

    for email in &config.to_emails {
        builder = builder.to(email.parse().unwrap());
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

    // Send the email
    match mailer.send(&email) {
        Ok(_) => Ok(()),
        Err(e) => Err(Box::new(e)),
    }
}

async fn get_show_ids(config: &Config) -> Result<Vec<i32>, Box<dyn Error>> {
    let pg_connection_string = &config.pg_connection_string;
    let builder = SslConnector::builder(SslMethod::tls())?;
    let connector = MakeTlsConnector::new(builder.build());

    let (client, connection) = tokio_postgres::connect(&pg_connection_string, connector).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move { connection.await });

    let ids: Vec<i32> = client
        .query("SELECT id FROM shows", &[])
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    Ok(ids)
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
    let today = Local::now().date_naive();
    let future_date_limit = today
        .checked_add_days(Days::new(FUTURE_LIMIT_DAYS))
        .unwrap();
    for show_handle in show_handles {
        if let Some(show) = show_handle.await?.unwrap() {
            if show.show_time.date_naive() > future_date_limit {
                continue;
            }
            shows.push(show)
        }
    }
    shows.sort_by(|a, b| a.show_time.cmp(&b.show_time));
    Ok(shows)
}
