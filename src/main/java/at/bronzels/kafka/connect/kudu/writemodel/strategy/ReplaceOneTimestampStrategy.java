package at.bronzels.kafka.connect.kudu.writemodel.strategy;

import at.bronzels.libcdcdw.Constants;
import at.bronzels.libcdcdw.kudu.pool.MyKudu;
import at.grahsl.kafka.connect.converter.SinkDocument;
import at.bronzels.libcdcdw.kudu.KuduOperation;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;
import org.bson.BsonDocument;

import java.time.Instant;

public class ReplaceOneTimestampStrategy extends WriteModelStrategy {

    //private static final ;
    public ReplaceOneTimestampStrategy(boolean isSrcFieldNameWTUpperCase) {
        super(isSrcFieldNameWTUpperCase);
    }

    @Override
    public Operation createWriteModel(SinkDocument document, MyKudu myKudu, Schema valueSchema) {

        BsonDocument vd = document.getValueDoc().orElseThrow(
                () -> new DataException("error: cannot build the WriteModel since"
                        + " the value document was missing unexpectedly")
        );

        Upsert upsert = myKudu.getKuduTable().newUpsert();
        PartialRow row = upsert.getRow();

        KuduOperation.haveRowAddedBy(row, vd, isSrcFieldNameWTUpperCase);

        long currmilli = Instant.now().toEpochMilli();
        row.addObject(Constants.FIELDNAME_MODIFIED_TS, currmilli);

        return upsert;
    }
}
