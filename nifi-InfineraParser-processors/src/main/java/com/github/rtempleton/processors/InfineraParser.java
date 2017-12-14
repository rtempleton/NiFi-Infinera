/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.rtempleton.processors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@Tags({"Infinera"})
@CapabilityDescription("Pivots Infinera files into a canonical format")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class InfineraParser extends AbstractProcessor {

	public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
			.Builder().name("MY_PROPERTY")
			.displayName("My property")
			.description("Example Property")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("Successfully parsed output")
			.build();

	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure")
			.description("The original file if parsing errors occured")
			.build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
//		descriptors.add(MY_PROPERTY);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if ( flowFile == null ) {
			return;
		}


		InputStream in = session.read(flowFile);
		BufferedReader br=new BufferedReader(new InputStreamReader(in));
		

		FlowFile targetFlowFile = session.create(flowFile);
		targetFlowFile = session.write(targetFlowFile, new OutputStreamCallback() {

			@Override
			public void process(OutputStream out) throws IOException {
				
				String nodeName = "", nodeId = "", section ="", module="", measure = "";
				Boolean valid = false;
//				final TimestampParser parser = new TimestampParser("foo", "yyyy.MM.dd.HH.mm.ss", new Timestamp(1999, 1, 1, 0, 0, 0, 0));
				Timestamp ts;
//				FloatParser fparser = new FloatParser("bar", null, null);
				Float val;

				boolean parsing = false;
				String[] header = new String[1];

				for (String line = br.readLine(); line != null; line = br.readLine()){ 

					if(line.startsWith("NODEID")) {
						String[] s = line.split(",");
						s = s[1].split("@");
						nodeName = s[0];
						nodeId = s[1];
						continue;
					}else if(line.startsWith("h_") && !line.startsWith("h_CONTROL")) {
						header = line.split(",");
						section = header[0];
//						System.out.println("file: " + section);
						parsing=true;
						continue;
					}else if (parsing==true){
						String[] s = line.split(",");
						if(s.length==1)
							continue;
						module = s[0];
						ts = formatTimestamp(s[2]);
						Integer v = Integer.parseInt(s[3].trim());
						valid = (v==0 || v%2==0) ? false : true;
						for(int i=5;i<s.length;i++) {
							measure = header[i];
							val = formatFloat(s[i]);
							//don't write out records with null values
							if(val==null)
								continue;
							String o = new String(nodeName + ","+ nodeId+","+section+","+module+","+ts+","+valid+","+measure+","+val+"\n");
							out.write(o.getBytes());
						}
					}
				}
				out.flush();
			}

		});
		
		try {
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		session.transfer(targetFlowFile, REL_SUCCESS);
		session.remove(flowFile);

	}
	
	private Timestamp formatTimestamp(String ts) {
		final DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy.MM.dd.HH.mm.ss").withOffsetParsed().withPivotYear(2000).withChronology(ISOChronology.getInstance());
		DateTime dt = dtf.parseDateTime(ts.trim());
		return new Timestamp(dt.getMillis());
	}
	
	private Float formatFloat(String f) {
		try {
			if(f.trim().length()==0)
				return null;
			return Float.parseFloat(f);
		}catch(Exception e) {
			return null;
		}
	}
}
