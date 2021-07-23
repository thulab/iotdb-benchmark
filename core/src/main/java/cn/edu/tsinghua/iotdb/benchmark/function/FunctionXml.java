package cn.edu.tsinghua.iotdb.benchmark.function;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "functions")
public class FunctionXml {
    private List<FunctionParam> functions = new ArrayList<FunctionParam>();

    @XmlElement(name = "function")
    public List<FunctionParam> getFunctions() {
        return functions;
    }

    public void setFunctions(List<FunctionParam> functions) {
        this.functions = functions;
    }
}
