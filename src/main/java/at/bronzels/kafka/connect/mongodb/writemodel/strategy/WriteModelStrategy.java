package at.bronzels.kafka.connect.mongodb.writemodel.strategy;

import at.grahsl.kafka.connect.converter.SinkDocument;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;

public interface WriteModelStrategy {

    WriteModel<BsonDocument> createWriteModel(SinkDocument document);

}
