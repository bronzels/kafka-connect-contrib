package at.bronzels.kafka.connect.kudu.writemodel.strategy;

import at.grahsl.kafka.connect.converter.SinkDocument;
import at.bronzels.libcdcdw.kudu.KuduOperation;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;
import org.bson.BsonDocument;

public class ReplaceOneDefaultStrategy extends WriteModelStrategy {

    //private static final ;
    public ReplaceOneDefaultStrategy(boolean isSrcFieldNameWTUpperCase) {
        super(isSrcFieldNameWTUpperCase);
    }

    @Override
    public Operation createWriteModel(SinkDocument document, KuduTable table, Schema valueSchema) {

        BsonDocument vd = document.getValueDoc().orElseThrow(
                () -> new DataException("error: cannot build the WriteModel since"
                        + " the value document was missing unexpectedly")
        );

        Upsert upsert = table.newUpsert();
        PartialRow row = upsert.getRow();

        KuduOperation.haveRowAddedBy(row, vd, isSrcFieldNameWTUpperCase);

        return upsert;
    }
}
