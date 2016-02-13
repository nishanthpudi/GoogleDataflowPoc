package com.example.nishanth.dataflow.pipeline;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;

/**
 * Created by Nishanth on 2/13/2016.
 */
public class MinimalWordCount {
    public static void main(String[] args){
        DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        options.setRunner(BlockingDataflowPipelineRunner.class);
        options.setProject("gcp-dataflow");
        options.setStagingLocation("gs://gcp-dataflow/poc/wordcount/windows/");

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(TextIO.Read.from("gs://dataflow-samples/shakespeare/kinglear.txt"))
                .apply(ParDo.named("Extract Words").of(new DoFn<String, String>(){
                    @Override
                    public void processElement(ProcessContext context){
                        for (String word : context.element().toString().split("[^a-zA-Z']+")){
                            context.output(word);
                        }
                    }
                }))
                .apply(Count.<String>perElement())
                .apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV element){
                        return element.getKey() + "," + element.getValue();
                    }
                }))
                .apply(TextIO.Write.to("gs://gcp-dataflow/poc/wordcount/windows/output/count.txt"));
        pipeline.run();
    }
}
