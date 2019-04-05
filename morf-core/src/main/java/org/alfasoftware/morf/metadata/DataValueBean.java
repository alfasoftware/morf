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
    final int prime = 31;
    int result = 1;
    result = prime * result + (name == null ? 0 : name.hashCode());
    result = prime * result + (value == null ? 0 : value.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    DataValueBean other = (DataValueBean) obj;
    if (name == null) {
      if (other.name != null) return false;
    } else if (!name.equals(other.name)) return false;
    if (value == null) {
      if (other.value != null) return false;
    } else if (!value.equals(other.value)) return false;
    return true;
  }
}