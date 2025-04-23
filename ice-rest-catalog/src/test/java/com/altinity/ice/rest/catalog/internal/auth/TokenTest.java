package com.altinity.ice.rest.catalog.internal.auth;

import static org.testng.Assert.*;

import java.util.Map;
import org.testng.annotations.Test;

public class TokenTest {

  @Test
  public void testParse() {
    assertEquals(Token.parse(""), new Token("", "", Map.of()));
    assertEquals(Token.parse("foo"), new Token("", "foo", Map.of()));
    assertEquals(Token.parse("foo:"), new Token("", "foo", Map.of()));
    assertEquals(Token.parse(":foo"), new Token("", "foo", Map.of()));
    assertEquals(Token.parse(":foo:"), new Token("", "foo", Map.of()));
    assertEquals(Token.parse("::"), new Token("", "", Map.of()));
    assertEquals(Token.parse("alias:foo"), new Token("alias", "foo", Map.of()));
    assertEquals(Token.parse("alias:foo:"), new Token("alias", "foo", Map.of()));
    assertEquals(
        Token.parse(String.format("alias:foo:%s=s", Token.TOKEN_PARAM_AWS_ASSUME_ROLE_ARN)),
        new Token("alias", "foo", Map.of(Token.TOKEN_PARAM_AWS_ASSUME_ROLE_ARN, "s")));
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*unknown token parameter.*")
  public void testParseUnknownParameter() {
    assertEquals(Token.parse("alias:foo:a=s"), new Token("alias", "foo", Map.of("a", "s")));
  }
}
