package at.bronzels.kafka.connect.kudu.writemodel.strategy;

import at.grahsl.kafka.connect.converter.SinkDocument;

import org.apache.kafka.connect.data.Schema;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;

abstract public class WriteModelStrategy {

    protected boolean isSrcFieldNameWTUpperCase = false;

    public WriteModelStrategy(boolean isSrcFieldNameWTUpperCase) {
        this.isSrcFieldNameWTUpperCase = isSrcFieldNameWTUpperCase;
    }

    abstract public Operation createWriteModel(SinkDocument document, KuduTable table, Schema valueSchema);

}
