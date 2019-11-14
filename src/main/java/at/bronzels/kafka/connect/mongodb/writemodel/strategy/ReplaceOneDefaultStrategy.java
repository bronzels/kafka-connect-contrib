package at.bronzels.kafka.connect.mongodb.writemodel.strategy;

import at.grahsl.kafka.connect.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

public class ReplaceOneDefaultStrategy implements WriteModelStrategy {

    private static final UpdateOptions UPDATE_OPTIONS =
                                    new UpdateOptions().upsert(true);

    @Override
    public WriteModel<BsonDocument> createWriteModel(SinkDocument document) {

        BsonDocument vd = document.getValueDoc().orElseThrow(
                () -> new DataException("error: cannot build the WriteModel since"
                        + " the value document was missing unexpectedly")
        );

        return new ReplaceOneModel<>(
                new BsonDocument(at.bronzels.libcdcdw.Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME,
                        vd.get(at.bronzels.libcdcdw.Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME)),
                vd,
                UPDATE_OPTIONS);

    }
}
