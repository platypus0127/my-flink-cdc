package com.hsb.utils;

import org.junit.Test; 
import org.junit.Before; 
import org.junit.After; 

/** 
* JobConfig Tester. 
* 
* @author <Authors name> 
* @since <pre>2ÔÂ 11, 2022</pre> 
* @version 1.0 
*/ 
public class JobConfigTest { 

@Before
public void before() throws Exception {

} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: reader(Properties props, String filePath) 
* 
*/ 
@Test
public void testReader() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getIntVal(String key) 
* 
*/ 
@Test
public void testGetIntVal() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getProperty(String key) 
* 
*/ 
@Test
public void testGetProperty() throws Exception { 
//TODO: Test goes here...
    String property = JobConfig.getProperty("impala.jdbc");
    System.out.println(property);
} 


} 
