# brave-context-rxjava2
`CurrentTraceContextAssemblyTracking` prevents traces from breaking
during RxJava operations by scoping them with trace context.

The design of this library borrows heavily from https://github.com/akaita/RxJava2Debug

To set this up, create `CurrentTraceContextAssemblyTracking` using the
current trace context provided by your `Tracing` component.

```java
assemblyTracking = CurrentTraceContextAssemblyTracking.create(
  tracing.currentTraceContext()
);
```

After your application-specific changes to `RxJavaPlugins`, enable trace
context tracking like so:

```java
assemblyTracking.enable();
```
