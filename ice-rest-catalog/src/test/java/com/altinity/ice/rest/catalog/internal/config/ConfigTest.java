package com.altinity.ice.rest.catalog.internal.config;

import static org.testng.Assert.*;

import java.util.Map;
import org.testng.annotations.Test;

public class ConfigTest {

  @Test
  public void testParseQuery() {
    assertEquals(Config.parseQuery(""), Map.of());
    assertEquals(Config.parseQuery("k"), Map.of("k", ""));
    assertEquals(Config.parseQuery("k=v"), Map.of("k", "v"));
    assertEquals(Config.parseQuery("k=v&x"), Map.of("k", "v", "x", ""));
    assertEquals(Config.parseQuery("k=a&k=b"), Map.of("k", "a,b"));
  }
}
