/*
 * Workaround: Scala 2.12 cannot pick between Mockito.spy(T) and varargs overloads.
 * Java resolves Mockito.spy(real) to the single-argument overload.
 */
package net.snowflake.spark.snowflake;

import org.mockito.Mockito;

public final class MockitoCompat {
  private MockitoCompat() {}

  public static JDBCWrapper spyJdbcWrapper(JDBCWrapper real) {
    return Mockito.spy(real);
  }
}
