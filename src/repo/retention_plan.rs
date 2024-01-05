use chrono::Duration;
use regex::Regex;
use std::{fmt, str::FromStr};

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
        let re = Regex::new(r"(\d+)\*(\d+)([dhw])").unwrap();
        let captures = re.captures(s).ok_or("Invalid retention plan: {*s}")?;

        let instances: i32 = captures[1]
            .parse()
            .map_err(|_e| "Invalid period string: {captures[1]}")?;
        let multiplier: i64 = captures[2]
            .parse()
            .map_err(|_e| "Invalid period string: {captures[2]}")?;
        let unit = &captures[3];

        let interval = match unit {
            "m" => Duration::minutes(multiplier),
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

fn duration_to_str(duration: Duration) -> String {
    let minutes = duration.num_minutes();
    const MINUTES_PER_HOUR: i64 = 60;
    const MINUTES_PER_DAY: i64 = MINUTES_PER_HOUR * 24;
    const MINUTES_PER_WEEK: i64 = MINUTES_PER_DAY * 7;

    if minutes % MINUTES_PER_WEEK == 0 {
        return format!("{}w", minutes / MINUTES_PER_WEEK);
    }
    if minutes % MINUTES_PER_DAY == 0 {
        return format!("{}d", minutes / MINUTES_PER_DAY);
    }
    if minutes % MINUTES_PER_HOUR == 0 {
        return format!("{}h", minutes / MINUTES_PER_HOUR);
    }

    return format!("{}m", minutes);
}

impl fmt::Display for RetentionPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let periods_fmt: Vec<String> = self
            .periods
            .iter()
            .map(|p| format!("{}*{}", p.instances, duration_to_str(p.interval)))
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
