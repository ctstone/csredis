using System;
using System.Diagnostics;

namespace ctstone.Redis
{
    class ActivityTracer : IDisposable
    {
        private Guid _oldActivityId;
        private static Lazy<TraceSource> _source;

        public static TraceSource Source { get { return _source.Value; } }
        public static string SourceName { get; set; }

        static ActivityTracer()
        {
            _source = new Lazy<TraceSource>(() => new TraceSource(SourceName ?? "csredis"));
        }

        public ActivityTracer()
            : this("Main")
        { }
        public ActivityTracer(string operation)
        {
            _oldActivityId = Trace.CorrelationManager.ActivityId;
            if (_oldActivityId == Guid.Empty)
                Trace.CorrelationManager.ActivityId = Guid.NewGuid();
            Trace.CorrelationManager.StartLogicalOperation(operation);
        }

        public void Dispose()
        {
            if (Trace.CorrelationManager.LogicalOperationStack.Count > 0)
                Trace.CorrelationManager.StopLogicalOperation();
            if (_oldActivityId == Guid.Empty)
                Trace.CorrelationManager.ActivityId = _oldActivityId;
        }

        public static void Verbose(string format, params object[] args)
        {
            Source.TraceEvent(TraceEventType.Verbose, 0, format, args);
        }

        public static void Info(string format, params object[] args)
        {
            Source.TraceEvent(TraceEventType.Information, 0, format, args);
        }

        public static void Warn(string format, params object[] args)
        {
            Source.TraceEvent(TraceEventType.Warning, 0, format, args);
        }

        public static void Error(string format, params object[] args)
        {
            Source.TraceEvent(TraceEventType.Error, 0, format, args);
        }

        public static void Error(Exception exception)
        {
            Source.TraceData(TraceEventType.Error, 0, exception);
        }

        public static void Warn(Exception exception)
        {
            Source.TraceData(TraceEventType.Warning, 0, exception);
        }
    }
}
