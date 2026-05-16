import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, ParentBased, TraceIdRatioBased


def _build_sampler():
    name = os.environ.get("OTEL_TRACES_SAMPLER", "parentbased_always_on")
    try:
        ratio = float(os.environ.get("OTEL_TRACES_SAMPLER_ARG", "1.0"))
    except ValueError:
        ratio = 1.0

    if name == "parentbased_traceidratio":
        return ParentBased(TraceIdRatioBased(ratio))
    if name == "traceidratio":
        return TraceIdRatioBased(ratio)
    return ALWAYS_ON


def _build_resource() -> Resource:
    attrs: dict = {SERVICE_NAME: os.environ.get("OTEL_SERVICE_NAME", "main-api")}
    for pair in os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "").split(","):
        if "=" in pair:
            k, v = pair.split("=", 1)
            attrs[k.strip()] = v.strip()
    return Resource.create(attrs)


def setup_tracing() -> None:
    provider = TracerProvider(resource=_build_resource(), sampler=_build_sampler())

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://tempo:4317")
    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))

    trace.set_tracer_provider(provider)

    if os.environ.get("OTEL_PYTHON_LOG_CORRELATION", "false").lower() == "true":
        LoggingInstrumentor().instrument(set_logging_format=True)


def get_tracer() -> trace.Tracer:
    return trace.get_tracer("main-api")
