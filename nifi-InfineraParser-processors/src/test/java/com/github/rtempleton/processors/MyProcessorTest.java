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

import java.io.File;
import java.io.IOException;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;


public class MyProcessorTest {

    private TestRunner testRunner;

    @Ignore
//    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(InfineraParser.class);
    }

    @Ignore
    public void testProcessor() throws IOException {
    	
    		File f = new File("./src/test/resources/FOO_pm15min_20170915.153937_2.csv").getAbsoluteFile();
    		System.out.println(f);
    		testRunner.enqueue(f.toPath());
    		testRunner.run();
    		
    		final MockFlowFile mff = testRunner.getFlowFilesForRelationship("success").get(0);
    		String s = new String(mff.toByteArray(), "UTF-8");
    		System.out.println(s);

    }
    
    @Test
    public void importTest() {
    	
    }

}
