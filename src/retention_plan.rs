pub struct RetentionPlan {
    periods: Vec<Period>,
}

pub struct Period {
    instances : i32,
    interval: std::time::Duration,
}



