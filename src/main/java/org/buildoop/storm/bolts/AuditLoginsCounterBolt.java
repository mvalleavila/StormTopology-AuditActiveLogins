/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.buildoop.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

import static backtype.storm.utils.Utils.tuple;

@SuppressWarnings("serial")
public class AuditLoginsCounterBolt implements IBasicBolt {
	
	private String hbaseTableName;
	
	public AuditLoginsCounterBolt(String hbaseTableName){
		this.hbaseTableName = hbaseTableName;
	}


    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
    }

    /*
     * Just output the word value with a count of 1.
     * The HBaseBolt will handle incrementing the counter.
     */
    public void execute(Tuple input, BasicOutputCollector collector){
    	Map<String,String> auditAttributes = (Map<String, String>) input.getValues().get(0);
    	String host_user = new String(auditAttributes.get("host")).concat("|")
    			.concat(auditAttributes.get("user"));
    	String type = auditAttributes.get("type");
    	if (type.equals("USER_LOGIN")){
            collector.emit(tuple(host_user, 1));
    	}
    	else if (type.equals("USER_LOGOUT")){
    		try {
				if (isCounterGreatThanZero(this.hbaseTableName,host_user))
					collector.emit(tuple(host_user, -1));
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("host-user", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
    private boolean isCounterGreatThanZero(String hbaseTableName, String hbaseRowName) throws IOException{
		Configuration config = HBaseConfiguration.create();
		HTable table = new HTable(config, hbaseTableName);
		Get g = new Get(Bytes.toBytes(hbaseRowName));
		Result r = table.get(g);
		byte [] value = r.getValue(Bytes.toBytes("activeLogins"),Bytes.toBytes("count"));
		long valueLong = Bytes.toLong(value);
		table.close();
    	if (valueLong > 0)    	
    		return true;
    	else
    		return false;
    }

}
