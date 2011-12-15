/**
 * Autogenerated by Thrift Compiler (0.7.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package gridool.sqlet.api;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum ErrorType implements org.apache.thrift.TEnum {
  PARSE(0),
  EXECUTION(1),
  UNSUPPORTED(2);

  private final int value;

  private ErrorType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static ErrorType findByValue(int value) { 
    switch (value) {
      case 0:
        return PARSE;
      case 1:
        return EXECUTION;
      case 2:
        return UNSUPPORTED;
      default:
        return null;
    }
  }
}
