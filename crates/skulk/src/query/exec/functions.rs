//! Prometheus-compatible float range-vector functions.

use super::{FloatSample, InstantSample, RangeVectorValue};
use crate::model::Timestamp;
use crate::query::plan::{RangeFunctionKind, SeriesWindow};

const NANOSECONDS_PER_SECOND: f64 = 1_000_000_000.0;

pub(super) fn evaluate(
    function: RangeFunctionKind,
    input: RangeVectorValue,
    output_timestamp: Timestamp,
) -> Vec<InstantSample> {
    let RangeVectorValue { series, window } = input;
    series
        .into_iter()
        .filter_map(|series| {
            evaluate_series(function, series.samples(), window).map(|value| InstantSample {
                series: series.series,
                evaluation_timestamp: output_timestamp,
                source_timestamp: output_timestamp,
                value,
                drop_metric_name: true,
            })
        })
        .collect()
}

fn evaluate_series(
    function: RangeFunctionKind,
    samples: &[FloatSample],
    window: SeriesWindow,
) -> Option<f64> {
    match function {
        RangeFunctionKind::Rate => extrapolated_rate(samples, window, true),
        RangeFunctionKind::IRate => instant_rate(samples),
        RangeFunctionKind::Increase => extrapolated_rate(samples, window, false),
        RangeFunctionKind::AvgOverTime => average_over_time(samples),
        RangeFunctionKind::MinOverTime => minimum_over_time(samples),
        RangeFunctionKind::MaxOverTime => maximum_over_time(samples),
        RangeFunctionKind::SumOverTime => sum_over_time(samples),
        RangeFunctionKind::CountOverTime => (!samples.is_empty()).then_some(samples.len() as f64),
    }
}

/// Port of Prometheus `extrapolatedRate` for float samples.
fn extrapolated_rate(
    samples: &[FloatSample],
    window: SeriesWindow,
    per_second: bool,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }

    let first = samples.first()?;
    let last = samples.last()?;
    let mut result = last.value() - first.value();
    for pair in samples.windows(2) {
        let previous = pair[0].value();
        let current = pair[1].value();
        if current < previous {
            result += previous;
        }
    }

    let range_start = window.evaluation_time.checked_sub(window.duration_ns)?;
    let mut duration_to_start = seconds_between(first.timestamp(), range_start);
    let mut duration_to_end = seconds_between(window.evaluation_time, last.timestamp());
    let sampled_interval = seconds_between(last.timestamp(), first.timestamp());
    let average_duration_between_samples = sampled_interval / (samples.len() - 1) as f64;
    let extrapolation_threshold = average_duration_between_samples * 1.1;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_duration_between_samples / 2.0;
    }

    let duration_to_zero = if result > 0.0 && first.value() >= 0.0 {
        sampled_interval * (first.value() / result)
    } else {
        duration_to_start
    };
    if duration_to_zero < duration_to_start {
        duration_to_start = duration_to_zero;
    }

    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_duration_between_samples / 2.0;
    }

    let mut factor = 1.0;
    if sampled_interval != 0.0 {
        factor = (sampled_interval + duration_to_start + duration_to_end) / sampled_interval;
    }
    if per_second {
        factor /= window.duration_ns as f64 / NANOSECONDS_PER_SECOND;
    }
    Some(result * factor)
}

/// Port of Prometheus `instantValue(..., isRate=true)` for float samples.
fn instant_rate(samples: &[FloatSample]) -> Option<f64> {
    let previous = samples.get(samples.len().checked_sub(2)?)?;
    let current = samples.last()?;
    let sampled_interval = seconds_between(current.timestamp(), previous.timestamp());
    if sampled_interval == 0.0 {
        return None;
    }
    let difference = if current.value() < previous.value() {
        current.value()
    } else {
        current.value() - previous.value()
    };
    Some(difference / sampled_interval)
}

fn average_over_time(samples: &[FloatSample]) -> Option<f64> {
    let first = samples.first()?.value();
    let mut sum = first;
    let mut count = 1.0;
    let mut mean = 0.0;
    let mut compensation = 0.0;
    let mut incremental_mean = false;

    for (index, sample) in samples.iter().enumerate().skip(1) {
        count = (index + 1) as f64;
        if !incremental_mean {
            let (new_sum, new_compensation) = compensated_add(sample.value(), sum, compensation);
            if !new_sum.is_infinite() {
                sum = new_sum;
                compensation = new_compensation;
                continue;
            }
            incremental_mean = true;
            mean = sum / (count - 1.0);
            compensation /= count - 1.0;
        }
        let previous_weight = (count - 1.0) / count;
        (mean, compensation) = compensated_add(
            sample.value() / count,
            previous_weight * mean,
            previous_weight * compensation,
        );
    }

    if incremental_mean {
        Some(mean + compensation)
    } else {
        Some(sum / count + compensation / count)
    }
}

fn minimum_over_time(samples: &[FloatSample]) -> Option<f64> {
    let mut minimum = samples.first()?.value();
    for sample in samples {
        let value = sample.value();
        if value < minimum || minimum.is_nan() {
            minimum = value;
        }
    }
    Some(minimum)
}

fn maximum_over_time(samples: &[FloatSample]) -> Option<f64> {
    let mut maximum = samples.first()?.value();
    for sample in samples {
        let value = sample.value();
        if value > maximum || maximum.is_nan() {
            maximum = value;
        }
    }
    Some(maximum)
}

fn sum_over_time(samples: &[FloatSample]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let (sum, compensation) = samples
        .iter()
        .fold((0.0, 0.0), |(sum, compensation), sample| {
            compensated_add(sample.value(), sum, compensation)
        });
    if sum.is_infinite() {
        Some(sum)
    } else {
        Some(sum + compensation)
    }
}

/// Prometheus's Kahan summation with the Neumaier improvement.
fn compensated_add(increment: f64, sum: f64, mut compensation: f64) -> (f64, f64) {
    let total = sum + increment;
    if total.is_infinite() {
        compensation = 0.0;
    } else if sum.abs() >= increment.abs() {
        compensation += (sum - total) + increment;
    } else {
        compensation += (increment - total) + sum;
    }
    (total, compensation)
}

fn seconds_between(later: Timestamp, earlier: Timestamp) -> f64 {
    (i128::from(later) - i128::from(earlier)) as f64 / NANOSECONDS_PER_SECOND
}
