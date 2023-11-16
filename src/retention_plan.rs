use chrono::Duration;
use regex::Regex;
use std::{fmt, str::FromStr};

// #[derive(Clone, Debug)]
// pub enum Duration {
//     Hours(i32),
//     Days(i32),
//     Weeks(i32),
//     Months(i32),
// }

#[derive(Clone, Debug)]
pub struct RetentionPlan {
    pub periods: Vec<Period>,
}

#[derive(Clone, Debug)]
pub struct Period {
    // How man backups to keep with this interval.
    pub instances: i32,
    // The duration.
    pub interval: Duration,
}

impl RetentionPlan {
    pub fn default() -> RetentionPlan {
        return "24*1h,7*1d".parse().unwrap();
    }
}

impl FromStr for Period {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let re = Regex::new(r"(\d+)\*(\d+)([dhwm])").unwrap();
        let captures = re.captures(s).ok_or("Invalid retention plan: {*s}")?;

        let instances: i32 = captures[1]
            .parse()
            .map_err(|_e| "Invalid period string: {captures[1]}")?;
        let multiplier: i64 = captures[2]
            .parse()
            .map_err(|_e| "Invalid period string: {captures[2]}")?;
        let unit = &captures[3];

        let interval = match unit {
            "d" => Duration::days(multiplier),
            "h" => Duration::hours(multiplier),
            "w" => Duration::weeks(multiplier),
            _ => return Err(String::from("Invalid period string: {unit}")),
        };
        return Ok(Period {
            instances,
            interval,
        });
    }
}

impl fmt::Display for RetentionPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let periods_fmt: Vec<String> = self
            .periods
            .iter()
            .map(|p| format!("{}*{}", p.instances, p.interval))
            .collect();
        write!(f, "{}", periods_fmt.join(","))
    }
}

impl FromStr for RetentionPlan {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let periods: Result<Vec<Period>, String> =
            s.split(',').map(|s| s.parse::<Period>()).collect();
        Ok(RetentionPlan { periods: periods? })
    }
}
