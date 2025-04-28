package com.altinity.ice.internal.cmd;

public record InsertOptions(
    boolean skipDuplicates,
    boolean noCommit,
    boolean noCopy,
    boolean forceNoCopy,
    boolean forceTableAuth,
    boolean s3NoSignRequest,
    boolean s3CopyObject,
    int threadCount) {

  public static Builder builder() {
    return new Builder();
  }

  public boolean skipDuplicates() {
    return skipDuplicates;
  }

  public boolean noCommit() {
    return noCommit;
  }

  public boolean noCopy() {
    return noCopy;
  }

  public boolean forceNoCopy() {
    return forceNoCopy;
  }

  public boolean forceTableAuth() {
    return forceTableAuth;
  }

  public boolean s3NoSignRequest() {
    return s3NoSignRequest;
  }

  public boolean s3CopyObject() {
    return s3CopyObject;
  }

  public int threadCount() {
    return threadCount;
  }

  public Builder toBuilder() {
    return builder()
        .skipDuplicates(skipDuplicates)
        .noCommit(noCommit)
        .noCopy(noCopy)
        .forceNoCopy(forceNoCopy)
        .forceTableAuth(forceTableAuth)
        .s3NoSignRequest(s3NoSignRequest)
        .s3CopyObject(s3CopyObject)
        .threadCount(threadCount);
  }

  public static final class Builder {
    private boolean skipDuplicates;
    private boolean noCommit;
    private boolean noCopy;
    private boolean forceNoCopy;
    private boolean forceTableAuth;
    private boolean s3NoSignRequest;
    private boolean s3CopyObject;
    private int threadCount = Runtime.getRuntime().availableProcessors();

    private Builder() {}

    public Builder skipDuplicates(boolean skipDuplicates) {
      this.skipDuplicates = skipDuplicates;
      return this;
    }

    public Builder noCommit(boolean noCommit) {
      this.noCommit = noCommit;
      return this;
    }

    public Builder noCopy(boolean noCopy) {
      this.noCopy = noCopy;
      return this;
    }

    public Builder forceNoCopy(boolean forceNoCopy) {
      this.forceNoCopy = forceNoCopy;
      return this;
    }

    public Builder forceTableAuth(boolean forceTableAuth) {
      this.forceTableAuth = forceTableAuth;
      return this;
    }

    public Builder s3NoSignRequest(boolean s3NoSignRequest) {
      this.s3NoSignRequest = s3NoSignRequest;
      return this;
    }

    public Builder s3CopyObject(boolean s3CopyObject) {
      this.s3CopyObject = s3CopyObject;
      return this;
    }

    public Builder threadCount(int threadCount) {
      this.threadCount = threadCount;
      return this;
    }

    public InsertOptions build() {
      return new InsertOptions(
          skipDuplicates,
          noCommit,
          noCopy,
          forceNoCopy,
          forceTableAuth,
          s3NoSignRequest,
          s3CopyObject,
          threadCount);
    }
  }
}
