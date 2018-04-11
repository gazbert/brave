package brave.grpc;

import brave.ErrorParser;
import brave.Tracing;
import brave.propagation.Propagation;
import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.ServerInterceptor;

public final class GrpcTracing {
  public static GrpcTracing create(Tracing tracing) {
    if (tracing == null) throw new NullPointerException("tracing == null");
    return new Builder(tracing).build();
  }

  public static Builder newBuilder(Tracing tracing) {
    return new Builder(tracing);
  }

  public static final class Builder {
    final Tracing tracing;
    GrpcClientParser clientParser;
    GrpcServerParser serverParser;
    boolean censusPropagationEnabled = false;

    Builder(Tracing tracing) {
      this.tracing = tracing;
      // override to re-use any custom error parser from the tracing component
      ErrorParser errorParser = tracing.errorParser();
      clientParser = new GrpcClientParser() {
        @Override protected ErrorParser errorParser() {
          return errorParser;
        }
      };
      serverParser = new GrpcServerParser() {
        @Override protected ErrorParser errorParser() {
          return errorParser;
        }
      };
    }

    public Builder clientParser(GrpcClientParser clientParser) {
      if (clientParser == null) throw new NullPointerException("clientParser == null");
      this.clientParser = clientParser;
      return this;
    }

    public Builder serverParser(GrpcServerParser serverParser) {
      if (serverParser == null) throw new NullPointerException("serverParser == null");
      this.serverParser = serverParser;
      return this;
    }

    /**
     * Use this to interop with processes that use <a href="https://github.com/census-instrumentation/opencensus-specs/blob/master/encodings/BinaryEncoding.md">
     * Census Binary Encoded Span Contexts</a>. Default (false) ignores this data.
     *
     * <p>When true, the "grpc-trace-bin" metadata key is parsed on server calls. When missing, {@link Tracing#propagation() normal propagation}
     * is used. During client calls, "grpc-trace-bin" is speculatively written.
     */
    public Builder censusPropagationEnabled(boolean censusPropagationEnabled) {
      this.censusPropagationEnabled = censusPropagationEnabled;
      return this;
    }

    public GrpcTracing build() {
      return new GrpcTracing(this);
    }
  }

  final Tracing tracing;
  final Propagation<Metadata.Key<String>> propagation;
  final GrpcClientParser clientParser;
  final GrpcServerParser serverParser;

  GrpcTracing(Builder builder) { // intentionally hidden constructor
    this.tracing = builder.tracing;
    this.propagation = builder.censusPropagationEnabled
        ? CensusGrpcPropagation.create(tracing.propagationFactory())
        : tracing.propagationFactory().create(AsciiMetadataKeyFactory.INSTANCE);
    this.clientParser = builder.clientParser;
    this.serverParser = builder.serverParser;
  }

  public Builder toBuilder() {
    return new Builder(tracing)
        .censusPropagationEnabled(propagation instanceof CensusGrpcPropagation)
        .clientParser(clientParser)
        .serverParser(serverParser);
  }

  /** This interceptor traces outbound calls */
  public final ClientInterceptor newClientInterceptor() {
    return new TracingClientInterceptor(this);
  }

  /** This interceptor traces inbound calls */
  public ServerInterceptor newServerInterceptor() {
    return new TracingServerInterceptor(this);
  }
}
