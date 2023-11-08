use regex::Regex;
use std::{fmt, str::FromStr};

#[derive(Clone, Debug)]
pub enum Duration {
    Hours(i32),
    Days(i32),
    Weeks(i32),
    Months(i32),
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Duration::Hours(hours) => write!(f, "{hours}h"),
            Duration::Days(days) => write!(f, "{days}d"),
            Duration::Weeks(weeks) => write!(f, "{weeks}w"),
            Duration::Months(months) => write!(f, "{months}m"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RetentionPlan {
    periods: Vec<Period>,
}

#[derive(Clone, Debug)]
pub struct Period {
    // How man backups to keep with this interval.
    instances: i32,
    // The duration.
    interval: Duration,
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
        let multiplier: i32 = captures[2]
            .parse()
            .map_err(|_e| "Invalid period string: {captures[2]}")?;
        let unit = &captures[3];

        let interval = match unit {
            "d" => Duration::Days(multiplier),
            "h" => Duration::Hours(multiplier),
            "w" => Duration::Weeks(multiplier),
            "m" => Duration::Months(multiplier),
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
