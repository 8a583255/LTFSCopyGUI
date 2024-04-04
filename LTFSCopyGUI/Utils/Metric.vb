Imports Prometheus

Public Class Metric
    Public Shared Property OperationProcessedGauge As Gauge
    Public Shared Property TotalFilesProcessedGauge As Gauge
    Public Shared Property CurrentFilesProcessedGauge As Gauge
    Public Shared Property TotalBytesProcessedGauge As Gauge
    Public Shared Property CurrentBytesProcessedGauge As Gauge
    Public Shared Property ErrorGauge As Gauge
    Public Shared Property OperationCounter As Counter
    Public Shared Property PreReadCounter As Counter
    Public Shared Property PreReadCounterInThread As Counter

    Public Shared Property FileOperationDurationHistogram As Histogram

    Public Shared Property FileOperationDurationSummary As Summary

    Shared Sub New()
        OperationProcessedGauge = Metrics.CreateGauge("operation_processed", "operation_processed", New GaugeConfiguration With {
    .LabelNames = New String() {"barcode", "operation"}
    })
        CurrentFilesProcessedGauge = Metrics.CreateGauge("current_files_processed", "current_files_processed", New GaugeConfiguration With {
    .LabelNames = New String() {"barcode"}
})
        TotalFilesProcessedGauge = Metrics.CreateGauge("total_files_processed", "total_files_processed", New GaugeConfiguration With {
    .LabelNames = New String() {"barcode"}})

        CurrentBytesProcessedGauge = Metrics.CreateGauge("current_bytes_processed", "current_bytes_processed", New GaugeConfiguration With {
    .LabelNames = New String() {"barcode"}})

        TotalBytesProcessedGauge = Metrics.CreateGauge("total_bytes_processed", "total_bytes_processed", New GaugeConfiguration With {
    .LabelNames = New String() {"barcode"}})
        ErrorGauge = Metrics.CreateGauge("error_msg", "error_msg", New GaugeConfiguration With {
    .LabelNames = New String() {"error", "msg"}})
        PreReadCounter = Metrics.CreateCounter("pre_read_count", "pre_read_count", New CounterConfiguration With {
.LabelNames = New String() {"barcode"}})
        PreReadCounterInThread = Metrics.CreateCounter("pre_read_count_in_thread", "pre_read_count_in_thread", New CounterConfiguration With {
.LabelNames = New String() {"barcode"}})
        OperationCounter= Metrics.CreateCounter("operation_counter", "operation_counter", New CounterConfiguration With {
                                                   .LabelNames = New String() {"operation"}})

        FileOperationDurationHistogram = Metrics.CreateHistogram("file_operation_duration_miliseconds", "file_read_duration_seconds",
    New HistogramConfiguration With {
        .LabelNames = New String() {"barcode", "operation", "singleblock"},
        .Buckets = New Double() {0.001,0.01,0.1,0.5,1, 2, 3, 5, 10, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 2000, 5000, 10000} ' 这里定义了直方图的桶配置
    })
        FileOperationDurationSummary = Metrics.CreateSummary("file_operation_duration_summary_seconds", "file_write_duration_summary_seconds",
    New SummaryConfiguration With {
        .LabelNames = New String() {"barcode", "operation", "singleblock"}
    })

    End Sub
    
    Public Shared Sub FuncFileOperationDuration(action As Action,labelValues As String() )
        Dim startTimestamp1 = DateTime.Now
        action()
        Dim duration1 As TimeSpan = DateTime.Now - startTimestamp1
        Metric.FileOperationDurationHistogram.WithLabels(labelValues).Observe(duration1.TotalMilliseconds)
        Metric.FileOperationDurationSummary.WithLabels(labelValues).Observe(duration1.TotalMilliseconds)
    End Sub
End Class
