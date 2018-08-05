package com.octo.datarepexam;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.tika.ParseResult;
import org.apache.beam.sdk.io.tika.TikaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class GCPDataflowSample {

    final private static String[] dsCols = {
            "DateTenancyCommenced",
            "DateLodgement",
            "BondAmount",
            "PremisesWeeklyRent",
            "PremisesDwellingType",
            "NumberBedrooms",
            "Premises_Postcode",
            "Premises_Suburb"};

    public interface DataflowOptions extends GcpOptions {

        @Description("Path to the file to read from")
        @Default.String("gs://datarep_data/source/fa13-2016-17.xlsx")
        String getSource();
        void setSource(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();
        void setOutput(String value);
    }

    private static class ExtractCSVBody extends DoFn<ParseResult, List<String>> {

        @ProcessElement
        public void processElement(@Element ParseResult element, OutputReceiver<List<String>> receiver) {
            String trimmedElement = element.getContent().trim();
            String[] rows = trimmedElement.split("\n");
            for (int i = 3; i < rows.length; i++) {
                receiver.output(Arrays.asList(rows[i].trim().split("\t")));
            }
        }

    }

    private static TableSchema getTableSchema() {
        List<TableFieldSchema> tableFields = Arrays.stream(dsCols)
                .map(col -> new TableFieldSchema().setName(col).setType("STRING"))
                .collect(Collectors.toList());
        return new TableSchema().setFields(tableFields);
    }

    private static TableRow formatBigQueryRow(List<String> values) {
        final TableRow row = new TableRow();
        int max = dsCols.length <= values.size() ? dsCols.length : values.size();
        for (int i = 0; i < max; i++) {
            row.set(dsCols[i], values.get(i));
        }
        return row;
    }

    public static void main(String[] args) {
        DataflowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(DataflowOptions.class);
        Pipeline p = Pipeline.create(options);
        PCollection<ParseResult> files = p.apply("Reading source dataset", TikaIO.parse().filepattern(options.getSource()));

        files.apply("Parsing source data", ParDo.of(new ExtractCSVBody()))
                .apply("Writing data to BigQuery", BigQueryIO.<List<String>>write()
                        .to(options.getProject() + ":" + options.getOutput())
                        .withSchema(getTableSchema())
                        .withFormatFunction(GCPDataflowSample::formatBigQueryRow)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        p.run().waitUntilFinish();
    }

}
