package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.iotdb.benchmark.workload.WorkloadException;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeviceSchema implements Cloneable{

    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceSchema.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    /** prefix of device name */
    private static final String DEVICE_NAME_PREFIX = "d_";
    /** Each device belongs to one group, i.e. database */
    private String group;
    /** Id of device */
    private String device;
    /** Sensor ids of device */
    private List<String> sensors;
    /** Only used for synthetic data set */
    private int deviceId;

    public DeviceSchema(){}

    public DeviceSchema(int deviceId) {
        this.deviceId = deviceId;
        this.device = DEVICE_NAME_PREFIX + deviceId;
        sensors = new ArrayList<>();
        try {
            createEvenlyAllocDeviceSchema();
        } catch (WorkloadException e) {
            LOGGER.error("Create device schema failed.", e);
        }
    }

    public DeviceSchema(String group, String device, List<String> sensors) {
        this.group = config.getDB_NAME() + "_" + group;
        this.device = DEVICE_NAME_PREFIX + device;
        this.sensors = sensors;
    }

    private void createEvenlyAllocDeviceSchema() throws WorkloadException {
        int thisDeviceGroupIndex = calGroupId(deviceId);
        group = config.getDB_NAME() + thisDeviceGroupIndex;
        sensors.addAll(config.getSENSOR_CODES());
    }

    int calGroupId(int deviceId) throws WorkloadException {
        switch (config.getSG_STRATEGY()) {
            case Constants.MOD_SG_ASSIGN_MODE:
                return deviceId % config.getGROUP_NUMBER();
            case Constants.HASH_SG_ASSIGN_MODE:
                return (deviceId + "").hashCode() % config.getGROUP_NUMBER();
            case Constants.DIV_SG_ASSIGN_MODE:
                int devicePerGroup = config.getDEVICE_NUMBER() / config.getGROUP_NUMBER();
                return (deviceId / devicePerGroup) % config.getGROUP_NUMBER();
            default:
                throw new WorkloadException("Unsupported SG_STRATEGY: " + config.getSG_STRATEGY());
        }
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public int getDeviceId() {
        return deviceId;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public List<String> getSensors() {
        return sensors;
    }

    public void setSensors(List<String> sensors) {
        this.sensors = sensors;
    }

    /**
     * serialize to output stream
     *
     * @param outputStream output stream
     */
    public void serialize(ByteArrayOutputStream outputStream) throws IOException {
        ReadWriteIOUtils.write(group, outputStream);
        ReadWriteIOUtils.write(device, outputStream);
        ReadWriteIOUtils.write(sensors.size(), outputStream);
        for (String sensor : sensors) {
            ReadWriteIOUtils.write(sensor, outputStream);
        }
        ReadWriteIOUtils.write(deviceId, outputStream);
    }

    /**
     * deserialize from input stream
     *
     * @param inputStream input stream
     */
    public static DeviceSchema deserialize(ByteArrayInputStream inputStream) throws IOException {
        DeviceSchema result = new DeviceSchema();
        result.group = ReadWriteIOUtils.readString(inputStream);
        result.device = ReadWriteIOUtils.readString(inputStream);
        result.sensors = ReadWriteIOUtils.readStringList(inputStream);
        result.deviceId = ReadWriteIOUtils.readInt(inputStream);

        return result;
    }

    @Override
    public String toString() {
        return "DeviceSchema{" +
                "group='" + group + '\'' +
                ", device='" + device + '\'' +
                ", sensors=" + sensors +
                ", deviceId=" + deviceId +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof DeviceSchema)) {
            return false;
        }

        DeviceSchema that = (DeviceSchema) o;

        return new EqualsBuilder()
                .append(deviceId, that.deviceId)
                .append(group, that.group)
                .append(device, that.device)
                .append(sensors, that.sensors)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(group)
                .append(device)
                .append(sensors)
                .append(deviceId)
                .toHashCode();
    }

    @Override
    public Object clone() throws CloneNotSupportedException{
        return super.clone();
    }
}
