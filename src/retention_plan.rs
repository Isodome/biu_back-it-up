use regex::Regex;
use std::{str::FromStr, time::Duration};


#[derive(Clone)]
pub struct RetentionPlan {
    periods: Vec<Period>,
}

#[derive(Clone)]
pub struct Period {
    instances: i32,
    interval: std::time::Duration,
}

const SECONDS_PER_HOUR: u64 = 60 * 60;
const SECONDS_PER_DAY: u64 = SECONDS_PER_HOUR * 24;
const SECONDS_PER_WEEK: u64 = SECONDS_PER_DAY * 7;

fn parse_period(s: &str) -> Result<Period, String> {
    let re = Regex::new(r"(\d+)\*(\d+)([dhwm])").unwrap();
    let captures = re.captures(s).ok_or("Invalid period string: {s}")?;

    let instances: i32 = captures[1]
        .parse()
        .map_err(|_e| "Invalid period string: {s}")?;
    let multiplier: u64 = captures[2]
        .parse()
        .map_err(|_e| "Invalid period string: {s}")?;
    let unit = &captures[3];

    let interval = match unit {
        "d" => Duration::from_secs(multiplier * SECONDS_PER_DAY),
        "h" => Duration::from_secs(multiplier * SECONDS_PER_HOUR),
        "w" => Duration::from_secs(multiplier * SECONDS_PER_WEEK),
        _ => return Err(String::from("Invalid period string: {s}")),
    };
    return Ok(Period {
        instances,
        interval,
    });
}

impl FromStr for RetentionPlan {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let periods : Result<Vec<Period>, String> = s.split(',').map(|s| parse_period(s)).collect();
        Ok(RetentionPlan { periods: periods? })
    }
}
