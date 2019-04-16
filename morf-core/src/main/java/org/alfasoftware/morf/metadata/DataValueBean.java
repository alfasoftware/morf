package org.alfasoftware.morf.metadata;

/**
 * Bean implementation of {@link DataValue}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2019
 */
final class DataValueBean implements DataValue {

  private final CaseInsensitiveString name;
  private final Object value;

  DataValueBean(String name, Object value) {
    this(CaseInsensitiveString.of(name), value);
  }

  DataValueBean(CaseInsensitiveString name, Object value) {
    super();
    this.name = name;
    this.value = value;
  }

  @Override
  public CaseInsensitiveString getName() {
    return name;
  }

  @Override
  public Object getObject() {
    return value;
  }

  @Override
  public String toString() {
    return name + "=" + value;
  }

  @Override
  public int hashCode() {
    return DataValue.defaultHashCode(this);
  }

  @Override
  public boolean equals(Object obj) {
    return DataValue.defaultEquals(this, obj);
  }
}