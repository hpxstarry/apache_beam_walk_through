package org.apache.beam.examples;

import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class MyWindowedWordCount {

    public void buildAndRunPipeline() {
        Pipeline pipeline = Pipeline.create();

        String inputPath = "";
        String outputPath = "";
        PCollection<String> input = pipeline.apply(TextIO.read().from(inputPath))
                .apply(ParDo.of(new AddTimestampFn()));

        PCollection<String> windowedWords = input.apply(
                Window.into(FixedWindows.of(Duration.standardMinutes(5))));

        PCollection<KV<String, Long>> wordCount = windowedWords
                .apply(new WordCountFun());

        wordCount
                .apply(MapElements.via(new WordCount.FormatAsTextFn()))
                .apply(new WriteOneFilePerWindow(outputPath, 2));

        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            try {
                result.cancel();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    static class WordCountFun extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordFn()));
            return words.apply(Count.perElement());
        }
    }

    static class ExtractWordFn extends DoFn<String, String> {
        public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            String[] words = element.split(TOKENIZER_PATTERN, -1);
            for (String word : words) {
                if (!word.isEmpty()) {
                    receiver.output(word);
                }
            }
        }
    }

    static class AddTimestampFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            Instant randomTimestamp =
                    new Instant(
                            ThreadLocalRandom.current()
                                    .nextLong());

            /*
             * Concept #2: Set the data element with that timestamp.
             */
            receiver.outputWithTimestamp(element, randomTimestamp);
        }
    }

    public static void main(String[] args) {
        MyWindowedWordCount myWindowedWordCount = new MyWindowedWordCount();
        myWindowedWordCount.buildAndRunPipeline();
    }
}
