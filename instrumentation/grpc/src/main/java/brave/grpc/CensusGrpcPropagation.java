package brave.grpc;

import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import java.util.List;

final class CensusGrpcPropagation implements Propagation<Key<String>> {
  /**
   * This creates a compatible metadata key based on Census, except this extracts a brave trace
   * context as opposed to a census span context
   */
  static final Key<TraceContext> GRPC_TRACE_BIN =
      Key.of("grpc-trace-bin", new TraceContextBinaryMarshaller());

  /**
   * This stashes the tag context in "extra" so it isn't lost in brave eventhough brave doesn't read
   * it.
   */
  static final Key<Extra> GRPC_TAGS_BIN =
      Key.of("grpc-tags-bin", new Metadata.BinaryMarshaller<Extra>() {

        @Override public byte[] toBytes(Extra value) {
          return value.grpc_tags_bin;
        }

        @Override public Extra parseBytes(byte[] serialized) {
          return new Extra(serialized);
        }
      });

  static Propagation<Key<String>> create(Propagation.Factory delegate) {
    if (delegate == null) throw new NullPointerException("delegate == null");
    return new CensusGrpcPropagation(delegate.create(AsciiMetadataKeyFactory.INSTANCE));
  }

  final Propagation<Key<String>> delegate;
  final List<Key<String>> keys;

  CensusGrpcPropagation(Propagation<Key<String>> delegate) {
    this.delegate = delegate;
    this.keys = delegate.keys();
  }

  @Override public List<Key<String>> keys() {
    return keys;
  }

  @Override public <C> TraceContext.Injector<C> injector(Setter<C, Key<String>> setter) {
    if (setter == null) throw new NullPointerException("setter == null");
    return new CensusGrpcInjector<>(delegate.injector(setter));
  }

  static final class CensusGrpcInjector<C> implements TraceContext.Injector<C> {
    final TraceContext.Injector<C> delegate;

    CensusGrpcInjector(TraceContext.Injector<C> delegate) {
      this.delegate = delegate;
    }

    @Override public void inject(TraceContext traceContext, C carrier) {
      if (carrier == null) throw new NullPointerException("carrier == null");
      assert carrier instanceof Metadata;
      ((Metadata) carrier).put(GRPC_TRACE_BIN, traceContext);

      List<Object> extra = traceContext.extra();
      for (int i = 0, length = extra.size(); i < length; i++) {
        Object next = extra.get(i);
        if (next instanceof Extra) {
          ((Metadata) carrier).put(GRPC_TAGS_BIN, ((Extra) next));
          break;
        }
      }

      delegate.inject(traceContext, carrier);
    }
  }

  @Override public <C> TraceContext.Extractor<C> extractor(Getter<C, Key<String>> getter) {
    if (getter == null) throw new NullPointerException("getter == null");
    return new CensusGrpcExtractor(delegate.extractor(getter));
  }

  static final class CensusGrpcExtractor<C> implements TraceContext.Extractor<C> {
    final TraceContext.Extractor<C> delegate;

    CensusGrpcExtractor(TraceContext.Extractor<C> delegate) {
      this.delegate = delegate;
    }

    @Override public TraceContextOrSamplingFlags extract(C carrier) {
      if (carrier == null) throw new NullPointerException("carrier == null");
      assert carrier instanceof Metadata;

      TraceContext extracted = ((Metadata) carrier).get(GRPC_TRACE_BIN);
      Extra extra = ((Metadata) carrier).get(GRPC_TAGS_BIN);
      if (extracted != null) {
        if (extra != null) {
          return TraceContextOrSamplingFlags.newBuilder()
              .addExtra(extra)
              .context(extracted).build();
        }
        return TraceContextOrSamplingFlags.create(extracted);
      }

      return delegate.extract(carrier);
    }
  }

  static final class Extra { // hidden intentionally
    final byte[] grpc_tags_bin;

    Extra(byte[] grpc_tags_bin) {
      this.grpc_tags_bin = grpc_tags_bin;
    }

    @Override public String toString() {
      return "CensusGrpcPropagation{}";
    }
  }
}
