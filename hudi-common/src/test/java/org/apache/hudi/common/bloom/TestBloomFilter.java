/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.bloom;

import org.apache.hudi.common.util.hash.Hash;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.FileSystemTestUtils.readLastLineFromResourceFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link SimpleBloomFilter} and {@link HoodieDynamicBoundedBloomFilter}.
 */
public class TestBloomFilter {

  // name attribute is optional, provide a unique name for test
  // multiple parameters, uses Collection<Object[]>
  public static List<Arguments> bloomFilterTypeCodes() {
    return Arrays.asList(
        Arguments.of(BloomFilterTypeCode.SIMPLE.name()),
        Arguments.of(BloomFilterTypeCode.DYNAMIC_V0.name())
    );
  }

  @ParameterizedTest
  @MethodSource("bloomFilterTypeCodes")
  public void testAddKey(String typeCode) {
    List<String> inputs;
    int[] sizes = {100, 1000, 10000};
    for (int size : sizes) {
      inputs = new ArrayList<>();
      BloomFilter filter = getBloomFilter(typeCode, size, 0.000001, size * 10);
      for (int i = 0; i < size; i++) {
        String key = UUID.randomUUID().toString();
        inputs.add(key);
        filter.add(key);
      }
      for (java.lang.String key : inputs) {
        assertTrue(filter.mightContain(key), "Filter should have returned true for " + key);
      }
      for (int i = 0; i < 100; i++) {
        String randomKey = UUID.randomUUID().toString();
        if (inputs.contains(randomKey)) {
          assertTrue(filter.mightContain(randomKey), "Filter should have returned true for " + randomKey);
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("bloomFilterTypeCodes")
  public void testSerialize(String typeCode) {

    List<String> inputs;
    int[] sizes = {100, 1000, 10000};
    for (int size : sizes) {
      inputs = new ArrayList<>();
      BloomFilter filter = getBloomFilter(typeCode, size, 0.000001, size * 10);
      for (int i = 0; i < size; i++) {
        String key = UUID.randomUUID().toString();
        inputs.add(key);
        filter.add(key);
      }

      String serString = filter.serializeToString();
      BloomFilter recreatedBloomFilter = BloomFilterFactory
          .fromString(serString, typeCode);
      for (String key : inputs) {
        assertTrue(recreatedBloomFilter.mightContain(key), "Filter should have returned true for " + key);
      }
    }
  }

  public static List<Arguments> bloomFilterParams() {
    return Arrays.asList(
        Arguments.of("hadoop", BloomFilterTypeCode.SIMPLE.name(), 200, 0.000001, Hash.MURMUR_HASH, -1),
        Arguments.of("hadoop", BloomFilterTypeCode.SIMPLE.name(), 1000, 0.000001, Hash.MURMUR_HASH, -1),
        Arguments.of("hadoop", BloomFilterTypeCode.SIMPLE.name(), 5000, 0.000001, Hash.MURMUR_HASH, -1),
        Arguments.of("hadoop", BloomFilterTypeCode.SIMPLE.name(), 10000, 0.000001, Hash.MURMUR_HASH, -1),
        Arguments.of("hadoop", BloomFilterTypeCode.SIMPLE.name(), 5000, 0.000001, Hash.JENKINS_HASH, -1),
        Arguments.of("hadoop", BloomFilterTypeCode.DYNAMIC_V0.name(), 200, 0.000001, Hash.MURMUR_HASH, 1000),
        Arguments.of("hadoop", BloomFilterTypeCode.DYNAMIC_V0.name(), 1000, 0.000001, Hash.MURMUR_HASH, 5000),
        Arguments.of("hadoop", BloomFilterTypeCode.DYNAMIC_V0.name(), 1000, 0.000001, Hash.JENKINS_HASH, 5000),
        Arguments.of("hudi", BloomFilterTypeCode.SIMPLE.name(), 1000, 0.000001, Hash.MURMUR_HASH, -1),
        Arguments.of("hudi", BloomFilterTypeCode.SIMPLE.name(), 5000, 0.000001, Hash.MURMUR_HASH, -1),
        Arguments.of("hudi", BloomFilterTypeCode.DYNAMIC_V0.name(), 1000, 0.000001, Hash.MURMUR_HASH, 5000)
    );
  }

  @ParameterizedTest
  @MethodSource("bloomFilterParams")
  public void testDeserialize(String lib, String typeCode, int numEntries,
                              double errorRate, int hashType, int maxEntries) throws IOException {
    // When the "lib" = "hadoop", this tests the backwards compatibility so that Hudi's
    // {@link InternalBloomFilter} correctly reads the bloom filters serialized by Hadoop
    List<String> keyList = Arrays.stream(
            readLastLineFromResourceFile("/format/bloom-filter/hadoop/all_10000.keys.data").split(","))
        .collect(Collectors.toList());
    String serializedFilter;
    if ("hadoop".equals(lib)) {
      String fileName = (BloomFilterTypeCode.DYNAMIC_V0.name().equals(typeCode) ? "dynamic" : "simple")
          + "_" + numEntries
          + "_000001_"
          + (hashType == Hash.MURMUR_HASH ? "murmur" : "jenkins")
          + (BloomFilterTypeCode.DYNAMIC_V0.name().equals(typeCode) ? "_" + maxEntries : "")
          + ".bf.data";
      serializedFilter = readLastLineFromResourceFile("/format/bloom-filter/hadoop/" + fileName);
    } else {
      BloomFilter inputFilter = getBloomFilter(typeCode, numEntries, errorRate, maxEntries);
      for (String key : keyList) {
        inputFilter.add(key);
      }
      serializedFilter = inputFilter.serializeToString();
    }
    validateBloomFilter(
        serializedFilter, keyList, lib, typeCode, numEntries, errorRate, hashType, maxEntries);
  }

  BloomFilter getBloomFilter(String typeCode, int numEntries, double errorRate, int maxEntries) {
    if (typeCode.equalsIgnoreCase(BloomFilterTypeCode.SIMPLE.name())) {
      return BloomFilterFactory.createBloomFilter(numEntries, errorRate, -1, typeCode);
    } else {
      return BloomFilterFactory.createBloomFilter(numEntries, errorRate, maxEntries, typeCode);
    }
  }

  private void validateBloomFilter(String serializedFilter, List<String> keyList, String lib,
                                   String typeCode, int numEntries, double errorRate,
                                   int hashType, int maxEntries) {
    BloomFilter bloomFilter = BloomFilterFactory
        .fromString(serializedFilter, typeCode);
    for (String key : keyList) {
      assertTrue(bloomFilter.mightContain(key), "Filter should have returned true for " + key);
    }
    if ("hadoop".equals(lib) && hashType == Hash.MURMUR_HASH) {
      BloomFilter hudiBloomFilter = getBloomFilter(typeCode, numEntries, errorRate, maxEntries);
      for (String key : keyList) {
        hudiBloomFilter.add(key);
      }
      // Hadoop library-serialized bloom filter should be exactly the same as Hudi one,
      // unless we made our customization in the future
      assertEquals(hudiBloomFilter.serializeToString(), serializedFilter);
    }
  }
}
