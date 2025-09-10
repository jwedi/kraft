use std::collections::HashMap;
use opentelemetry::Context;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_zipkin::Propagator;
use crate::server::raftproto::{TracingContext, TracingContextEntry};

// Function for converting a span to a TracingContext
pub fn span_to_tracing_context(span: &Context, propagator: &Propagator) -> TracingContext {
    let mut carrier: HashMap<String, String> = std::collections::HashMap::new();
    propagator.inject_context(span, &mut carrier);
    let entries = carrier.iter().map(|(key, value)| TracingContextEntry {
        key: key.clone(),
        value: value.clone(),
    }).collect();
    TracingContext { entries }
}

// Function for converting a TracingContext to a span
pub fn tracing_context_to_span(tracing_context: &TracingContext, propagator: &Propagator) -> opentelemetry::Context {
    let mut carrier: HashMap<String, String> = std::collections::HashMap::new();
    for entry in &tracing_context.entries {
        carrier.insert(entry.key.clone(), entry.value.clone());
    }
    propagator.extract(&carrier)
}