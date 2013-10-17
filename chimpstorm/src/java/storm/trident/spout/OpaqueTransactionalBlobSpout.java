package com.infochimps.storm.trident.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.spout.ISpoutPartition;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class OpaqueTransactionalBlobSpout implements IOpaquePartitionedTridentSpout<Map, OpaqueTransactionalBlobSpout.SinglePartition, Map> {
  private static final Logger LOG = LoggerFactory.getLogger(OpaqueTransactionalBlobSpout.class);
  private IBlobStore _bs;
  private IRecordizer _rc = new LineRecordizer();
  private String _of = null;
  private StartPolicy _sp = StartPolicy.RESUME;

  public OpaqueTransactionalBlobSpout(IBlobStore blobStore) {
    _bs = blobStore;
  }

  public OpaqueTransactionalBlobSpout(IBlobStore blobStore, IRecordizer rc ) {
    _bs = blobStore;
    _rc = rc;
  }

  public OpaqueTransactionalBlobSpout(IBlobStore blobStore, StartPolicy startPolicy, String offset) {
    _bs = blobStore;
    _sp = startPolicy;
    _of = offset;
  }

  public OpaqueTransactionalBlobSpout(IBlobStore blobStore, IRecordizer rc, StartPolicy startPolicy, String offset) {
    _bs = blobStore;
    _rc = rc;
    _sp = startPolicy;
    _of = offset;
  }

  @Override
  public IOpaquePartitionedTridentSpout.Emitter<Map, SinglePartition, Map> getEmitter(Map conf, TopologyContext context){
    return new Emitter(conf, context, _bs, _rc, _sp, _of);
  }

  @Override
  public IOpaquePartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
    return new Coordinator();
  }

  @Override
  public Map getComponentConfiguration() {
    return null;
  }

  @Override
  public Fields getOutputFields() {
    return _rc.getFields();
  }

  public class Emitter implements
      IOpaquePartitionedTridentSpout.Emitter<Map, SinglePartition, Map> {
    private String _compId;
    private IBlobStore _blobStore;
    private IRecordizer _rec;
    String marker = null;
    private boolean initialized = false;
    private StartPolicy _startPolicy;
    private String _offset;

    public Emitter(Map conf, TopologyContext context, IBlobStore blobStore, IRecordizer rec, StartPolicy startPolicy, String offset) {
      _compId = context.getThisComponentId();
      _blobStore = blobStore;
      _rec = rec;
      _blobStore.initialize();
      _startPolicy = startPolicy;
      _offset = offset;

    }


    @Override
    public Map emitPartitionBatch(TransactionAttempt tx, TridentCollector collector, SinglePartition partition, Map lastPartitionMeta) {
      String txId = "" + tx.getTransactionId();
      LOG.debug(Utils.logString("emitPartitionBatch", _compId, txId, "START:", _blobStore.toString()));

      boolean currentBatchFailed = false;
      boolean lastBatchFailed = false;
      boolean isDataAvailable = true;

      try {
        // Last metadata can only be null if the initial batch failed after all records were read successfully.(one reason is timeouts).
        if (!initialized || lastPartitionMeta == null) {
          LOG.info(Utils.logString("emitPartitionBatch", _compId, txId, "Initializing", _blobStore.toString()));

          switch (_startPolicy) {
            case EARLIEST:
              marker = _bs.getFirstMarker();
              isDataAvailable = marker != null ? true : false; // if marker is null then there are no files in the bucket + prefix yet.
              LOG.info(Utils.logString("emitPartitionBatch", _compId, txId, "INITIALIZATION: Starting from beginning", marker, _blobStore.toString()));
              break;
            case EXPLICIT:
              marker = _offset;
              if (!_blobStore.isMarkerValid(marker)) {
                throw new RuntimeException(String.format("INITIALIZATION: Starting from offset. Inconsistent State. Offset [%s] is not valid. %s", marker, _blobStore.toString()));
              }
              LOG.info(Utils.logString("emitPartitionBatch", _compId, txId, "INITIALIZATION: Starting from offset.", _offset, _blobStore.toString()));
              break;
            case LATEST:
              marker = _bs.getLastMarker();
              // Nothing to read until next time.
              isDataAvailable = false;
              LOG.info(Utils.logString("emitPartitionBatch", _compId, txId, "INITIALIZATION: Starting at the end", marker, _blobStore.toString()));
              break;
            default:
              if (lastPartitionMeta != null) {
                marker = (String) lastPartitionMeta.get("marker");
                // Check for invalid state
                if (!_blobStore.isMarkerValid(marker)) {
                  throw new RuntimeException(String.format("INITIALIZATION: Resuming from offset. Inconsistent State found in Zookeeper. Zookeeper State [%s] is not valid. %s", marker, _blobStore.toString()));
                }
                LOG.info(Utils.logString("emitPartitionBatch", _compId, txId, "INITIALIZATION: Resuming from offset (from Zookeeper)", marker, _blobStore.toString()));
                isDataAvailable = false; //read from the next marker.
              } else {
                // If no marker is found in ZK, use LATEST policy.
                marker = _bs.getLastMarker();
                LOG.warn(Utils.logString("emitPartitionBatch", _compId, txId, "INITIALIZATION: Can't resume. No state found in Zookeeper !!. Starting at the end", marker, _blobStore.toString()));
                // Nothing to read until next time.
                isDataAvailable = false;
              }
          }

          // Download the actual file
          if (isDataAvailable) {
            Map<String, Object> context = new HashMap<String, Object>();
            context.put("txId", txId);
            context.put("compId", _compId);
            context.put("marker", marker);
            _rec.recordize(_blobStore.getBlob(marker, context), collector, context);
          }

          // Initialization successful. Don't run this block ever again.
          initialized = true;
        } else {

          String prevMarker = marker;
          if (lastPartitionMeta == null)
            throw new RuntimeException("Inconsistent state. Trident metadata cannot be null after initialization!");

          marker = (String) lastPartitionMeta.get("marker");
          lastBatchFailed = (Boolean) lastPartitionMeta.get("lastBatchFailed");


          // Update the marker if the last batch succeeded.
          if (!lastBatchFailed) {
            String tmp = _blobStore.getNextBlobMarker(marker);
            // marker stays same if no new files are available.
            marker = (tmp == null) ? marker : tmp;
            isDataAvailable = (tmp == null) ? false : true;
          }
          LOG.debug(Utils.logString("emitPartitionBatch", _compId, txId, "curr", marker, "prev", prevMarker, _blobStore.toString()));

          // Download the actual file
          if (isDataAvailable) {
            Map<String, Object> context = new HashMap<String, Object>();
            context.put("txId", txId);
            context.put("compId", _compId);
            context.put("marker", marker);
            _rec.recordize(_blobStore.getBlob(marker, context), collector, context);
          }
        }

      } catch (Throwable t) {
        //Catch everything that can go wrong.

        // These checks are to ensure that this block never blows up.
        String lastMeta = lastPartitionMeta == null? "null" : lastPartitionMeta.toString(); // lastPartitionMeta = null, when exception occurs for the first file.
        String s3Settings = _blobStore == null ? "null" : _blobStore.toString();
        String filename = marker == null ? "null" : marker;
        String tId = txId == null ? "null" : txId;
        String componentId = _compId == null ? "null" : _compId;

        LOG.error(Utils.logString("emitPartitionBatch", componentId, tId,"Error in reading file. Filename", filename, "lastPartitionMeta", lastMeta, "S3 settings",s3Settings), t);
        currentBatchFailed = true;
      }

      // Update the lastMeta.
      Map newPartitionMeta = new HashMap();
      newPartitionMeta.put("marker", marker);
      newPartitionMeta.put("lastBatchFailed", currentBatchFailed);
      return newPartitionMeta;
    }

    @Override
    public void refreshPartitions(List<SinglePartition> partitionResponsibilities) {
    }

    @Override
    public List<SinglePartition> getOrderedPartitions(Map allPartitionInfo) {
      // Need to provide at least one partition, otherwise it spins forever.
      ArrayList<SinglePartition> partition = new ArrayList<SinglePartition>();
      partition.add(new SinglePartition());
      return partition;
    }

    @Override
    public void close() {
    }
  }

  class Coordinator implements IOpaquePartitionedTridentSpout.Coordinator<Map> {
    @Override
    public boolean isReady(long txid) {
      return true;
    }
    @Override
    public Map getPartitionsForBatch() {
      return null;
    }
    @Override
    public void close() {
    }
  }

  class SinglePartition implements ISpoutPartition{
    @Override
    public String getId() {
      return "dummy";
    }
  }

}
