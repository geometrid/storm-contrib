package com.infochimps.storm.trident.spout;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
    Long chunkOffset = null;
    boolean fileDone = false;
    private boolean initialized = false;
    private StartPolicy _startPolicy;
    private String _offset;

    public final int MAX_CHUNK_SIZE = 100000;

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
      LOG.info(Utils.logString("emitPartitionBatch", _compId, txId, "START:", _blobStore.toString()));

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
              chunkOffset = 0L;
              fileDone = false;
              isDataAvailable = marker != null ? true : false; // if marker is null then there are no files in the bucket + prefix yet.
              LOG.info(Utils.logString("emitPartitionBatch", _compId, txId, "INITIALIZATION: Starting from beginning", marker, _blobStore.toString()));
              break;
            case EXPLICIT:
              break;
            case LATEST:
              break;
          }

          // Download the actual file
          if (isDataAvailable) {
            //TODO: refactor this code into a function
            Map<String, Object> context = new HashMap<String, Object>();
            context.put("txId", txId);
            context.put("compId", _compId);
            context.put("marker", marker);
            context.put("chunkOffset", chunkOffset );
            _rec.recordize( getChunk(marker, context,chunkOffset), collector, context);

            fileDone = (Boolean) context.get("fileDone");
          }

          // Initialization successful. Don't run this block ever again.
          initialized = true;
        } else {

          String prevMarker = marker;
          if (lastPartitionMeta == null)
            throw new RuntimeException("Inconsistent state. Trident metadata cannot be null after initialization!");

          marker = (String) lastPartitionMeta.get("marker");
          lastBatchFailed = (Boolean) lastPartitionMeta.get("lastBatchFailed");
          chunkOffset = (Long) lastPartitionMeta.get("chunkOffset");
          boolean fileDone = (Boolean) lastPartitionMeta.get("fileDone");


          // Update the marker if the last batch succeeded.
          if (!lastBatchFailed && fileDone ) {
            chunkOffset = 0L;
            String tmp = _blobStore.getNextBlobMarker(marker);
            // marker stays same if no new files are available.
            marker = (tmp == null) ? marker : tmp;
            isDataAvailable = (tmp == null) ? false : true;
          }
          LOG.info(Utils.logString("emitPartitionBatch", _compId, txId, "curr", marker, "prev", prevMarker, _blobStore.toString()));

          // Download the actual file
          if (isDataAvailable) {
            Map<String, Object> context = new HashMap<String, Object>();
            context.put("txId", txId);
            context.put("compId", _compId);
            context.put("marker", marker);

            context.put("chunkOffset", chunkOffset );
            _rec.recordize( getChunk(marker, context, chunkOffset), collector, context);
            fileDone = (Boolean) context.get("fileDone");
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
      newPartitionMeta.put("chunkOffset", chunkOffset );
      newPartitionMeta.put("fileDone", fileDone );
      return newPartitionMeta;
    }

    private InputStream getChunk(String blobMarker, Map<String, Object> context, Long offset ) {
      InputStream fileStream = _blobStore.getBlob(marker, context);

      //TODO: create a member
      byte[] buff = new byte[MAX_CHUNK_SIZE + 1];

      try {
        int bytesRead = fileStream.read( buff, chunkOffset.intValue(), MAX_CHUNK_SIZE );
        chunkOffset += bytesRead;

        context.put("fileDone", (bytesRead < MAX_CHUNK_SIZE ? true : false) );


      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }


      return new ByteArrayInputStream( buff );
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
