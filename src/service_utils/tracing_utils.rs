use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::Context;
use opentelemetry_zipkin::Propagator;
use std::collections::HashMap;

/// Tracing context entry for propagation
#[derive(Clone, Debug)]
pub struct TracingContextEntry {
    pub key: String,
    pub value: String,
}

/// Tracing context for span propagation
#[derive(Clone, Debug, Default)]
pub struct TracingContext {
    pub entries: Vec<TracingContextEntry>,
}

/// Convert a span to a TracingContext for propagation
pub fn span_to_tracing_context(span: &Context, propagator: &Propagator) -> TracingContext {
    let mut carrier: HashMap<String, String> = std::collections::HashMap::new();
    propagator.inject_context(span, &mut carrier);
    let entries = carrier
        .iter()
        .map(|(key, value)| TracingContextEntry {
            key: key.clone(),
            value: value.clone(),
        })
        .collect();
    TracingContext { entries }
}

/// Convert a TracingContext to a span
pub fn tracing_context_to_span(tracing_context: &TracingContext, propagator: &Propagator) -> opentelemetry::Context {
    let mut carrier: HashMap<String, String> = std::collections::HashMap::new();
    for entry in &tracing_context.entries {
        carrier.insert(entry.key.clone(), entry.value.clone());
    }
    propagator.extract(&carrier)
}
