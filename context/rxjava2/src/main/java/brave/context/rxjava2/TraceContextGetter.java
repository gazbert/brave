package brave.context.rxjava2;

import brave.propagation.TraceContext;

// TODO: rename and put to better use. We want to guard we aren't exactly the same
// context already, rather than blindly wrapping
interface TraceContextGetter {
  TraceContext traceContext();
}
