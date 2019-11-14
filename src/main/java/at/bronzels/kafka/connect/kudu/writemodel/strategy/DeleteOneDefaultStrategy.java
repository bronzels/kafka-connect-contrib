package at.bronzels.kafka.connect.kudu.writemodel.strategy;

import at.grahsl.kafka.connect.converter.SinkDocument;
import at.bronzels.libcdcdw.kudu.KuduOperation;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.bson.BsonDocument;

public class DeleteOneDefaultStrategy extends WriteModelStrategy {

    public DeleteOneDefaultStrategy(boolean isSrcFieldNameWTUpperCase) {
        super(isSrcFieldNameWTUpperCase);
    }

    @Override
    public Operation createWriteModel(SinkDocument document, KuduTable table, Schema valueSchema) {

        BsonDocument kd = document.getKeyDoc().orElseThrow(
                () -> new DataException("error: cannot build the WriteModel since"
                        + " the key document was missing unexpectedly")
        );

        Delete delete = table.newDelete();
        PartialRow row = delete.getRow();

        KuduOperation.haveRowAddedBy(row, kd, isSrcFieldNameWTUpperCase);
        return delete;
    }
}
