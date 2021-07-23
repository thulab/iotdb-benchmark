package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class DataSchemaTest {
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  @Test
  public void test(){
    testBalanceSplit();
  }

  void testBalanceSplit(){
    int preDeviceNum = config.getDEVICE_NUMBER();
    int preClientNum = config.getCLIENT_NUMBER();
    config.setDEVICE_NUMBER(100);
    config.setCLIENT_NUMBER(30);
    int mod = config.getDEVICE_NUMBER() % config.getCLIENT_NUMBER();
    int deviceNumEachClient = config.getDEVICE_NUMBER() / config.getCLIENT_NUMBER();
    config.initDeviceCodes();
    DataSchema dataSchema = DataSchema.getInstance();
    Map<Integer, List<DeviceSchema>> client2Schema = dataSchema.getClientBindSchema();
    for (int clientId : client2Schema.keySet()){
      int deviceNumInClient = client2Schema.get(clientId).size();
      if (clientId < mod){
        Assert.assertEquals(deviceNumEachClient+1,deviceNumInClient);
      }
      else {
        Assert.assertEquals(deviceNumEachClient,deviceNumInClient);
      }
    }
    config.setDEVICE_NUMBER(preDeviceNum);
    config.setCLIENT_NUMBER(preClientNum);
  }


}
