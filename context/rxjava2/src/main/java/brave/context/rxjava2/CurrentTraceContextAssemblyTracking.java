package brave.context.rxjava2;

import brave.propagation.CurrentTraceContext;
import brave.propagation.TraceContext;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.flowables.ConnectableFlowable;
import io.reactivex.functions.Function;
import io.reactivex.internal.fuseable.ScalarCallable;
import io.reactivex.observables.ConnectableObservable;
import io.reactivex.parallel.ParallelFlowable;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Prevents traces from breaking during RxJava operations by scoping them with trace context.
 *
 * <p>The design of this library borrows heavily from https://github.com/akaita/RxJava2Debug
 */
public final class CurrentTraceContextAssemblyTracking {
  /** Prevents overlapping plugin configuration */
  static final AtomicBoolean lock = new AtomicBoolean();

  final CurrentTraceContext currentTraceContext;

  CurrentTraceContextAssemblyTracking(CurrentTraceContext currentTraceContext) {
    if (currentTraceContext == null) throw new NullPointerException("currentTraceContext == null");
    this.currentTraceContext = currentTraceContext;
  }

  public static CurrentTraceContextAssemblyTracking create(CurrentTraceContext delegate) {
    return new CurrentTraceContextAssemblyTracking(delegate);
  }

  public void enable() {
    if (!lock.compareAndSet(false, true)) return;

    RxJavaPlugins.setOnObservableAssembly(
        new ConditionalOnCurrentTraceContextFunction<Observable>() {
          @Override Observable applyActual(Observable o, TraceContext ctx) {
            if (!(o instanceof Callable)) {
              return new TraceContextObservable(o, currentTraceContext, ctx);
            }
            if (o instanceof ScalarCallable) {
              return new TraceContextScalarCallableObservable(o, currentTraceContext, ctx);
            }
            return new TraceContextCallableObservable(o, currentTraceContext, ctx);
          }
        });

    RxJavaPlugins.setOnConnectableObservableAssembly(
        new ConditionalOnCurrentTraceContextFunction<ConnectableObservable>() {
          @Override ConnectableObservable applyActual(ConnectableObservable co, TraceContext ctx) {
            return new TraceContextConnectableObservable(co, currentTraceContext, ctx);
          }
        });

    RxJavaPlugins.setOnCompletableAssembly(
        new ConditionalOnCurrentTraceContextFunction<Completable>() {
          @Override Completable applyActual(Completable c, TraceContext ctx) {
            if (!(c instanceof Callable)) {
              return new TraceContextCompletable(c, currentTraceContext, ctx);
            }
            if (c instanceof ScalarCallable) {
              return new TraceContextScalarCallableCompletable(c, currentTraceContext, ctx);
            }
            return new TraceContextCallableCompletable(c, currentTraceContext, ctx);
          }
        });

    RxJavaPlugins.setOnSingleAssembly(new ConditionalOnCurrentTraceContextFunction<Single>() {
      @Override Single applyActual(Single s, TraceContext ctx) {
        if (!(s instanceof Callable)) {
          return new TraceContextSingle(s, currentTraceContext, ctx);
        }
        if (s instanceof ScalarCallable) {
          return new TraceContextScalarCallableSingle(s, currentTraceContext, ctx);
        }
        return new TraceContextCallableSingle(s, currentTraceContext, ctx);
      }
    });

    RxJavaPlugins.setOnMaybeAssembly(new ConditionalOnCurrentTraceContextFunction<Maybe>() {
      @Override Maybe applyActual(Maybe m, TraceContext ctx) {
        if (!(m instanceof Callable)) {
          return new TraceContextMaybe(m, currentTraceContext, ctx);
        }
        if (m instanceof ScalarCallable) {
          return new TraceContextScalarCallableMaybe(m, currentTraceContext, ctx);
        }
        return new TraceContextCallableMaybe(m, currentTraceContext, ctx);
      }
    });

    RxJavaPlugins.setOnFlowableAssembly(new ConditionalOnCurrentTraceContextFunction<Flowable>() {
      @Override Flowable applyActual(Flowable f, TraceContext ctx) {
        if (!(f instanceof Callable)) {
          return new TraceContextFlowable(f, currentTraceContext, ctx);
        }
        if (f instanceof ScalarCallable) {
          return new TraceContextScalarCallableFlowable(f, currentTraceContext, ctx);
        }
        return new TraceContextCallableFlowable(f, currentTraceContext, ctx);
      }
    });

    RxJavaPlugins.setOnConnectableFlowableAssembly(
        new ConditionalOnCurrentTraceContextFunction<ConnectableFlowable>() {
          @Override ConnectableFlowable applyActual(ConnectableFlowable cf, TraceContext ctx) {
            return new TraceContextConnectableFlowable(cf, currentTraceContext, ctx);
          }
        }
    );

    RxJavaPlugins.setOnParallelAssembly(
        new ConditionalOnCurrentTraceContextFunction<ParallelFlowable>() {
          @Override ParallelFlowable applyActual(ParallelFlowable pf, TraceContext ctx) {
            return new TraceContextParallelFlowable(pf, currentTraceContext, ctx);
          }
        });

    lock.set(false);
  }

  public static void disable() {
    if (!lock.compareAndSet(false, true)) return;

    RxJavaPlugins.setOnObservableAssembly(null);
    RxJavaPlugins.setOnConnectableObservableAssembly(null);
    RxJavaPlugins.setOnCompletableAssembly(null);
    RxJavaPlugins.setOnSingleAssembly(null);
    RxJavaPlugins.setOnMaybeAssembly(null);
    RxJavaPlugins.setOnFlowableAssembly(null);
    RxJavaPlugins.setOnConnectableFlowableAssembly(null);
    RxJavaPlugins.setOnParallelAssembly(null);

    lock.set(false);
  }

  abstract class ConditionalOnCurrentTraceContextFunction<T> implements Function<T, T> {
    @Override public final T apply(T t) {
      TraceContext ctx = currentTraceContext.get();
      if (ctx == null) return t; // less overhead when there's no current trace
      return applyActual(t, ctx);
    }

    abstract T applyActual(T t, TraceContext ctx);
  }
}
