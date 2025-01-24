
#[derive(PartialEq, Debug)]
pub enum ServiceError {
    ThrottlingError(String),
    RaceConditionError
}