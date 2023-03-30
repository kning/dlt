"""Implements SupportsTracking"""
import contextlib
from typing import Any
import humanize
from sentry_sdk import Hub
from sentry_sdk.tracing import Span

from dlt.common import pendulum
from dlt.common import logger
from dlt.common.runtime.exec_info import github_info
from dlt.common.runtime.segment import track as dlthub_telemetry_track
from dlt.common.runtime.slack import send_slack_message
from dlt.common.pipeline import LoadInfo, SupportsPipeline

from dlt.pipeline.typing import TPipelineStep
from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace


def _add_sentry_tags(span: Span, pipeline: SupportsPipeline) -> None:
    span.set_tag("pipeline_name", pipeline.pipeline_name)
    if pipeline.destination:
        span.set_tag("destination", pipeline.destination.__name__)
    if pipeline.dataset_name:
        span.set_tag("dataset_name", pipeline.dataset_name)


def slack_notify_load_success(incoming_hook: str, load_info: LoadInfo, trace: PipelineTrace) -> int:
    """Sends a markdown formatted success message and returns http status code from the Slack incoming hook"""
    try:
        author = github_info().get("github_user", "")
        if author:
            author = f":hard-hat:{author}'s "

        total_elapsed = pendulum.now() - trace.started_at

        def _get_step_elapsed(step: PipelineStepTrace) -> str:
            if not step:
                return ""
            elapsed = step.finished_at - step.started_at
            return f"`{step.step.upper()}`: _{humanize.precisedelta(elapsed)}_ "

        load_step = trace.steps[-1]
        normalize_step = next((step for step in trace.steps if step.step == "normalize"), None)
        extract_step = next((step for step in trace.steps if step.step == "extract"), None)

        message = f"""The {author}pipeline *{load_info.pipeline.pipeline_name}* just loaded *{len(load_info.loads_ids)}* load package(s) to destination *{load_info.destination_name}* and into dataset *{load_info.dataset_name}*.
🚀 *{humanize.precisedelta(total_elapsed)}* of which {_get_step_elapsed(load_step)}{_get_step_elapsed(normalize_step)}{_get_step_elapsed(extract_step)}"""

        send_slack_message(incoming_hook, message)
        return 200
    except Exception as ex:
        logger.warning(f"Slack notification could not be sent: {str(ex)}")
        return -1


def on_start_trace(trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> None:
    # https://getsentry.github.io/sentry-python/api.html#sentry_sdk.Hub.capture_event
    if pipeline.runtime_config.sentry_dsn:
        # print(f"START SENTRY TX: {trace.transaction_id} SCOPE: {Hub.current.scope}")
        transaction = Hub.current.start_transaction(name=step, op=step)
        _add_sentry_tags(transaction, pipeline)
        transaction.__enter__()


def on_start_trace_step(trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> None:
    if pipeline.runtime_config.sentry_dsn:
        # print(f"START SENTRY SPAN {trace.transaction_id}:{trace_step.span_id} SCOPE: {Hub.current.scope}")
        span = Hub.current.scope.span.start_child(description=step, op=step).__enter__()
        span.op = step
        _add_sentry_tags(span, pipeline)


def on_end_trace_step(trace: PipelineTrace, step: PipelineStepTrace, pipeline: SupportsPipeline, step_info: Any) -> None:
    if pipeline.runtime_config.sentry_dsn:
        # print(f"---END SENTRY SPAN {trace.transaction_id}:{step.span_id}: {step} SCOPE: {Hub.current.scope}")
        with contextlib.suppress(Exception):
            Hub.current.scope.span.__exit__(None, None, None)
    # disable automatic slack messaging until we can configure messages themselves
    # if step.step == "load":
    #     if pipeline.runtime_config.slack_incoming_hook and step.step_exception is None:
    #         slack_notify_load_success(pipeline.runtime_config.slack_incoming_hook, step_info, trace)
    dlthub_telemetry_track("pipeline", step.step, {
        "elapsed": (step.finished_at - trace.started_at).total_seconds(),
        "success": step.step_exception is None,
        "destination_name": pipeline.destination.__name__.split(".")[-1] if pipeline.destination else None,
        "transaction_id": trace.transaction_id
    })


def on_end_trace(trace: PipelineTrace, pipeline: SupportsPipeline) -> None:
    if pipeline.runtime_config.sentry_dsn:
        # print(f"---END SENTRY TX: {trace.transaction_id} SCOPE: {Hub.current.scope}")
        with contextlib.suppress(Exception):
            Hub.current.scope.span.__exit__(None, None, None)