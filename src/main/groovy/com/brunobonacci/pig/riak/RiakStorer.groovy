package com.brunobonacci.pig.riak

/*
  Copyright (c) 2013 Bruno Bonacci. All Rights Reserved.

  This file is part of the pig-riak project.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

Third-party Licenses:

  All third-party dependencies are listed in build.gradle.
*/

import groovyx.net.http.HTTPBuilder;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.fs.*;
import org.apache.pig.*;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.*;
import org.apache.pig.data.Tuple;
import java.io.*;
import java.util.*;

public class RiakStorer extends StoreFunc {
  protected HTTPBuilder _riak;
  protected String _fullpath;
  protected RecordWriter _writer;
  protected String _contentType;
  protected String _host;
  protected int _port;
  protected String _bucket;

  public RiakStorer() {
    this("text/plain","localhost",8098);
  }
  public RiakStorer(String contentType) {
    this(contentType,"localhost",8098);
  }
  public RiakStorer(String contentType, String host) {
    this(contentType,host,8098);
  }
  public RiakStorer(String contentType, String host, int port) {
    _host = host;
    _port = port;
    _contentType = contentType;
  }

  @Override
  public OutputFormat getOutputFormat() {
      return new NullOutputFormat();
  }

  @Override
  public void putNext(Tuple f) throws IOException {
      if(f.get(0) == null) {
        return;
      }

      String key = f.get(0).toString();
      Object value = f.getAll()?.get(1);
      if(value != null) {
          String content = value.toString()
          // if request fails an Exception is raised by HTTPClient
          _riak.post(
              path: key,
              body: content,
              requestContentType: _contentType )
     }
  }

  @Override
  public void prepareToWrite(RecordWriter writer) {
      _writer = writer;
      _fullpath = "http://$_host:$_port/riak/$_bucket"
      _riak = new HTTPBuilder( "$_fullpath/" )
  }

  @Override
    public void setStoreLocation(String location, Job job) throws IOException {
       if( !location || location.contains('/') )
           throw new IllegalArgumentException("Invalid bucket name $location");
       _bucket = location;
    }

  

  @Override
  public String relToAbsPathForStoreLocation(String location, org.apache.hadoop.fs.Path curDir) throws IOException {
       return location;
    }
}
