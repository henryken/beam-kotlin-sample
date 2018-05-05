package io.suryawirawan.henry.beam.sample

import mu.KotlinLogging
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.joda.time.DateTime
import java.io.Serializable

data class WebEvent(
        val eventTime: DateTime,
        val browser: String
) : Serializable

private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {

    val options = PipelineOptionsFactory.fromArgs(*args).create()
    val pipeline = Pipeline.create(options)

    val webEvents = Create.of(
            WebEvent(DateTime.now(), "IE8"),
            WebEvent(DateTime.now(), "Firefox"),
            WebEvent(DateTime.now(), "Chrome"),
            WebEvent(DateTime.now(), "IE8"),
            WebEvent(DateTime.now(), "Firefox"),
            WebEvent(DateTime.now(), "Chrome")
    )

    pipeline.apply(webEvents)
            .apply(ParDo.of(object: DoFn<WebEvent, KV<String, WebEvent>>() {
                @ProcessElement
                fun processElement(context: ProcessContext) {
                    val webEvent = context.element()
                    context.output(KV.of(webEvent.browser, webEvent))
                }
            }))
            .apply(Count.perKey())
            .apply(ParDo.of(object: DoFn<KV<String, Long>, String>() {
                @ProcessElement
                fun processElement(context: ProcessContext) {
                    logger.info(context.element().toString())
                }
            }))

    pipeline.run()
}