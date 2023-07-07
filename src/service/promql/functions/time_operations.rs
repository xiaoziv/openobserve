// Copyright 2023 Openobserve.ai and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::service::promql::value::{InstantValue, Labels, LabelsExt, Sample, Value};
use chrono::{Datelike, NaiveDate, Timelike};
use datafusion::error::{DataFusionError, Result};
use strum::EnumIter;

#[derive(Debug, EnumIter)]
pub enum TimeOperationType {
    Minute,
    Hour,
    DayOfWeek,
    DayOfMonth,
    DayOfYear,
    DaysInMonth,
    Month,
    Year,
}

impl TimeOperationType {
    /// Given a timestamp, get the TimeOperationType component from it
    /// for e.g. month(), year(), day() etc.
    pub fn get_component_from_ts(&self, timestamp: i64) -> u32 {
        let naive_datetime = chrono::NaiveDateTime::from_timestamp_micros(timestamp).unwrap();
        match self {
            Self::Minute => naive_datetime.minute(),
            Self::Hour => naive_datetime.hour(),
            Self::Month => naive_datetime.month(),
            Self::Year => naive_datetime.year() as u32,
            Self::DayOfWeek => naive_datetime.weekday().num_days_from_sunday(), // Starting from 0
            Self::DayOfMonth => naive_datetime.day(),
            Self::DayOfYear => naive_datetime.ordinal(), // Starting from 1
            Self::DaysInMonth => {
                let cur_month = naive_datetime.month();
                let cur_year = naive_datetime.year();
                let naive_date = if cur_month == 12 {
                    NaiveDate::from_ymd_opt(cur_year + 1, 1, 1)
                } else {
                    NaiveDate::from_ymd_opt(cur_year, cur_month + 1, 1)
                };
                naive_date
                    .unwrap()
                    .signed_duration_since(NaiveDate::from_ymd_opt(cur_year, cur_month, 1).unwrap())
                    .num_days() as u32
            }
        }
    }
}

pub(crate) fn minute(data: &Value) -> Result<Value> {
    exec(data, &TimeOperationType::Minute)
}

pub(crate) fn hour(data: &Value) -> Result<Value> {
    exec(data, &TimeOperationType::Hour)
}

pub(crate) fn month(data: &Value) -> Result<Value> {
    exec(data, &TimeOperationType::Month)
}

pub(crate) fn year(data: &Value) -> Result<Value> {
    exec(data, &TimeOperationType::Year)
}

pub(crate) fn day_of_month(data: &Value) -> Result<Value> {
    exec(data, &TimeOperationType::DayOfMonth)
}

pub(crate) fn day_of_week(data: &Value) -> Result<Value> {
    exec(data, &TimeOperationType::DayOfWeek)
}

pub(crate) fn day_of_year(data: &Value) -> Result<Value> {
    exec(data, &TimeOperationType::DayOfYear)
}

pub(crate) fn days_in_month(data: &Value) -> Result<Value> {
    exec(data, &TimeOperationType::DaysInMonth)
}

fn exec(data: &Value, op: &TimeOperationType) -> Result<Value> {
    match &data {
        Value::Vector(v) => {
            if v.is_empty() {
                let now = chrono::Utc::now().timestamp_micros();
                let instant = InstantValue {
                    labels: Labels::default(),
                    sample: Sample::new(now, 0.0),
                };
                return Ok(Value::Vector(vec![instant]));
            }

            let out = v
                .iter()
                .map(|instant| {
                    let ts = op.get_component_from_ts(instant.sample.timestamp);
                    InstantValue {
                        labels: instant.labels.without_metric_name(),
                        sample: Sample::new(instant.sample.timestamp, ts as f64),
                    }
                })
                .collect();
            Ok(Value::Vector(out))
        }
        Value::None => Ok(Value::None),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Invalid input for minute value: {:?}",
            data
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strum::IntoEnumIterator;

    #[test]
    fn test_get_component_from_ts() {
        let timestamp_micros = 1688379261000000; // Mon Jul 03 2023 10:14:21 GMT+0000

        let expected_outputs = [14, 10, 1, 3, 184, 31, 7]; // Strict ordering based on TimeOperationType
        for (op, expected) in std::iter::zip(TimeOperationType::iter(), expected_outputs) {
            let got = op.get_component_from_ts(timestamp_micros);
            assert!(
                got == expected,
                "operation type: {:?} expected {} got {}",
                op,
                expected,
                got
            );
        }
    }
}
