package simpledb.common;

/**
 * Class representing requested permissions to a relation/file.
 * Private constructor with two static objects READ_ONLY and READ_WRITE that
 * represent the two levels of permission.
 */
public enum Permissions {
    READ_ONLY, READ_WRITE
}
