package co.cts.benchmarks.dataflow.query3;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;

public class InputReader implements Serializable {
    private final String table;
    private final Schema schema;
    private final String filter;

    private InputReader(String table, Schema schema, String filter) {
        this.table = table;
        this.schema = schema;
        this.filter = filter;
    }

    private InputReader(String table, Schema schema) {
        this.table = table;
        this.schema = schema;
        this.filter = null;
    }

    public static BigQueryIO.TypedRead<Row> getItems() {
        Schema schema = Schema.builder()
                .addNullableField("i_brand_id", INT64)
                .addNullableField("i_brand", STRING)
                .addInt64Field("i_item_sk")
                .addNullableField("i_manufact_id", INT64)
                .build();

        InputReader inputReader = new InputReader("item", schema, "i_manufact_id = 128");
        return inputReader.getRows();
    }

    public static BigQueryIO.TypedRead<Row> getDates() {
        Schema mySchema = Schema.builder()
                .addInt64Field("d_year")
                .addInt64Field("d_date_sk")
                .addInt64Field("d_moy")
                .build();

        InputReader inputReader = new InputReader("date_dim", mySchema, "d_moy = 11");
        return inputReader.getRows();
    }

    public static BigQueryIO.TypedRead<Row> getStoreSales() {
        Schema mySchema = Schema.builder()
                .addNullableField("ss_sold_date_sk", INT64)
                .addNullableField("ss_item_sk", INT64)
                // Unfortunately Decimal is not supported!
                .addNullableField("ss_ext_sales_price", DOUBLE)
                .build();

        InputReader inputReader = new InputReader("store_sales", mySchema);
        return inputReader.getRows();
    }

    public BigQueryIO.TypedRead<Row> getRows() {
        BigQueryIO.TypedRead<Row> reader = BigQueryIO
                .read(extractRow())
                .from(Parameters.DATASET + "." + table)
                .withSelectedFields(schema.getFieldNames())
                .withCoder(RowCoder.of(schema))
                .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ);

        if (filter == null) {
            return reader;
        } else {
            // predicate pushdown
            return reader.withRowRestriction(filter);
        }
    }

    @NotNull
    private SerializableFunction<SchemaAndRecord, Row> extractRow() {
        return schemaAndRecord -> {
            GenericRecord record = schemaAndRecord.getRecord();
            return Row.withSchema(schema)
                    .addValues(extractColumns(record))
                    .build();
        };
    }

    private List<Object> extractColumns(GenericRecord record) {
        List<Object> columns = new ArrayList<>();

        for (Field field : schema.getFields()) {
            Object value = record.get(field.getName());
            columns.add(setType(field, value));
        }

        return columns;
    }

    private Object setType(Field field, Object value) {
        if (value == null) {
            return null;
        }

        TypeName definedType = field.getType().getTypeName();
        if (definedType.isStringType()) {
            return value.toString();
        } else if (definedType.equals(TypeName.DOUBLE)) {
            return new BigDecimal(new BigInteger(((ByteBuffer) value).array()), 9).doubleValue();
        } else {
            return value;
        }
    }
}
