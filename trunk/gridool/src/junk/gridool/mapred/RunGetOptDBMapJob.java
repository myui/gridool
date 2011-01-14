package gridool.mapred;

import gridool.GridClient;
import gridool.GridJob;
import gridool.GridTask;
import gridool.construct.GridJobBase;
import gridool.db.DBLocalJob;
import gridool.db.partitioning.monetdb.MonetDBTableAdvPartitioningBulkloadBatchTask;
import gridool.db.record.DBRecord;
import gridool.db.record.MultiKeyRowPlaceholderRecord;
import gridool.mapred.db.DBMapReduceJobConf;
import gridool.mapred.db.GetOptDBJobConf;
import gridool.mapred.db.task.DBMapShuffleTaskBase;

import java.rmi.RemoteException;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class RunGetOptDBMapJob {

    public static void main(String[] args) {
        final GetOptDBJobConf jobConf = new JobConf(args);
        final GridClient grid = new GridClient();
        try {
            grid.execute(DBLocalJob.class, jobConf);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    private static final class JobConf extends GetOptDBJobConf {
        private static final long serialVersionUID = -7235561431500455239L;

        public JobConf(String[] argv) {
            super(argv);
        }

        @Override
        public DBRecord createMapInputRecord() {
            return new MultiKeyRowPlaceholderRecord();
        }

        @SuppressWarnings("unchecked")
        @Override
        public DBMapShuffleTaskBase makeMapShuffleTask(GridJobBase<DBMapReduceJobConf, ?> job) {
            MonetDBTableAdvPartitioningBulkloadBatchTask task = new MonetDBTableAdvPartitioningBulkloadBatchTask(job, this);
            task.setShuffleUnits(100000);
            task.setShuffleThreads(-1); // workaround for monetdb (avoid concurrent insertion due to the table-level lock)
            return task;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected GridTask makeReduceTask(GridJob job, String inputTableName, String destTableName) {
            throw new UnsupportedOperationException();
        }

    }

}
