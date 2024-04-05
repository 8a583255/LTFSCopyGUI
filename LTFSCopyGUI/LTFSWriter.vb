Imports System.Collections.Concurrent
Imports System.ComponentModel
Imports System.IO
Imports System.Runtime.InteropServices
Imports System.Security.AccessControl
Imports System.Text
Imports Fsp.Interop
Imports Microsoft.VisualBasic.CompilerServices
Imports Microsoft.WindowsAPICodePack.Dialogs
Imports Newtonsoft.Json
Imports Castle.Windsor
Imports Castle.MicroKernel.Registration
Imports Castle.DynamicProxy

Public Class LTFSWriter
    Public Property TapeDrive As String = ""
    Public Property schema As ltfsindex
    Public Property plabel As New ltfslabel With {.blocksize = 524288}
    Public Property Modified As Boolean = False
    Public Property OfflineMode As Boolean = False
    Public Property IndexPartition As Byte = 0
    Public Property DataPartition As Byte = 1
    
    
    Public Property IsSqliteTreeView As Boolean = True
    Public Function GetPartitionNumber(partition As ltfslabel.PartitionLabel) As Byte
        If plabel Is Nothing Then Return partition
        If partition = plabel.partitions.index Then
            Return IndexPartition
        Else
            Return DataPartition
        End If
    End Function

    Public Property IndexWriteInterval As Long
        Get
            Return My.Settings.LTFSWriter_IndexWriteInterval
        End Get
        Set(value As Long)
            value = Math.Max(0, value)
            My.Settings.LTFSWriter_IndexWriteInterval = value
            If value = 0 Then
                索引间隔36GiBToolStripMenuItem.Text = My.Resources.ResText_NoIndex
            Else
                索引间隔36GiBToolStripMenuItem.Text = $"{My.Resources.ResText_IndexInterval}{IOManager.FormatSize(value)}"
            End If
            My.Settings.Save()
        End Set
    End Property

    public _TotalBytesUnindexed As Long
    Public Property TotalBytesUnindexed As Long
        Set(value As Long)
            _TotalBytesUnindexed = value
            If Not 更新数据区索引ToolStripMenuItem.Enabled AndAlso
                value <> 0 AndAlso schema IsNot Nothing AndAlso
                schema.location.partition = ltfsindex.PartitionLabel.b Then Invoke(Sub() 更新数据区索引ToolStripMenuItem.Enabled = True)
        End Set
        Get
            Return _TotalBytesUnindexed
        End Get
    End Property
    Public Property TotalBytesProcessed As Long = 0
    Public Property TotalFilesProcessed As Long = 0
    Public Property CurrentBytesProcessed As Long = 0
    Public Property CurrentFilesProcessed As Long = 0
    Private _CurrentHeight As Long = 0
    Public Property CurrentHeight
        Get
            return _CurrentHeight
        End Get
        Set
             _CurrentHeight =  value
        End Set
    End Property

    Public ReadOnly Property GetPos As TapeUtils.PositionData
        Get
            Return TapeUtils.ReadPosition(TapeDrive)
        End Get
    End Property
    Public Property ExtraPartitionCount As Long = 0
    Public Property CapReduceCount As Long = 0
    
    Private Shared privateSqliteDic As New ConcurrentDictionary(Of String, SQLite.SQLiteConnection)
    Private Shared privateSqliteTrDic As New ConcurrentDictionary(Of String, SQLite.SQLiteTransaction)
    Public Shared SqliteTransQueue As New ConcurrentQueue(Of Int32)
    Public Shared SqliteLock As New Object
    Public Shared Function GetSqliteConnection(barcode As String) As SQLite.SQLiteConnection
        If Not privateSqliteDic.ContainsKey(barcode) Then
            Dim conn = DirProvider.CreateConnection($"sqlite\{barcode}.db")

            privateSqliteDic.AddOrUpdate(barcode, conn, Function(key, oldValue) oldValue)
            conn.Open()
        End If
        Return privateSqliteDic(barcode)
    End Function
    Public Property CapacityRefreshInterval As Integer
        Get
            Return My.Settings.LTFSWriter_CapacityRefreshInterval
        End Get
        Set(value As Integer)
            value = Math.Max(0, value)
            My.Settings.LTFSWriter_CapacityRefreshInterval = value
            If value = 0 Then
                容量刷新间隔30sToolStripMenuItem.Text = My.Resources.ResText_CRDisabled
            Else
                容量刷新间隔30sToolStripMenuItem.Text = $"{My.Resources.ResText_CRIntv}{value}s"
            End If
        End Set
    End Property
    Private _SpeedLimit As Integer = 0
    Public Property SpeedLimit As Integer
        Set(value As Integer)
            value = Math.Max(0, value)
            _SpeedLimit = value
            If _SpeedLimit = 0 Then
                限速不限制ToolStripMenuItem.Text = My.Resources.ResText_NoSLim
            Else
                限速不限制ToolStripMenuItem.Text = $"{My.Resources.ResText_SLim}{_SpeedLimit} MiB/s"
            End If
        End Set
        Get
            Return _SpeedLimit
        End Get
    End Property
    Public Property SpeedLimitLastTriggerTime As Date = Now
    Public CheckCount As Integer = 0
    Public Property CheckCycle As Integer = 10
    Public Property CleanCycle
        Set(value)
            value = Math.Max(0, value)
            If value = 0 Then
                重装带前清洁次数3ToolStripMenuItem.Text = My.Resources.ResText_RBCoff
            Else
                重装带前清洁次数3ToolStripMenuItem.Text = $"{My.Resources.ResText_RBC}{value}"
            End If
            My.Settings.LTFSWriter_CleanCycle = value
            My.Settings.Save()
        End Set
        Get
            Return My.Settings.LTFSWriter_CleanCycle
        End Get
    End Property
    Public Property HashOnWrite As Boolean
        Get
            Return 计算校验ToolStripMenuItem.Checked
        End Get
        Set(value As Boolean)
            计算校验ToolStripMenuItem.Checked = value
        End Set
    End Property

    Public Property AllowOperation As Boolean = True
    Public OperationLock As New Object
    Public Property Barcode As String = ""
    Public Property StopFlag As Boolean = False
    Public Property Pause As Boolean = False
    Public Property Flush As Boolean = False
    Public Property Clean As Boolean = False
    Public Property Clean_last As Date = Now
    Public Property DisablePartition As Boolean
        Get
            Return My.Settings.LTFSWriter_DisablePartition
        End Get
        Set(value As Boolean)
            My.Settings.LTFSWriter_DisablePartition = value
            My.Settings.Save()
            TapeUtils.AllowPartition = Not DisablePartition
        End Set
    End Property
    Public Property Session_Start_Time As Date = Now
    Public logFile As String = IO.Path.Combine(Application.StartupPath, $"log\LTFSWriter_{Session_Start_Time.ToString("yyyyMMdd_HHmmss.fffffff")}.log")
    Public errorLogFile As String = IO.Path.Combine(Application.StartupPath, $"log\LTFSWriter_{Session_Start_Time.ToString("yyyyMMdd_HHmmss.fffffff")}.error.log")
    Public Property SilentMode As Boolean = False
    Public Property SilentAutoEject As Boolean = False
    Public BufferedBytes As Long = 0
    Private ddelta, fdelta, rwhdelta, rwtdelta As Long
    Public SMaxNum As Integer = 600
    Public PMaxNum As Integer = 3600 * 6
    Public SpeedHistory As List(Of Double) = New Double(PMaxNum) {}.ToList()
    Public NativeSpeedHistory As List(Of Double) = New Double(PMaxNum) {}.ToList()
    Public FileRateHistory As List(Of Double) = New Double(PMaxNum) {}.ToList()

    Public FileDroper As FileDropHandler
    Public Event LTFSLoaded()
    Public Event WriteFinished()
    Public Event TapeEjected()
    Public Shared Sub FuncSqliteTrans(action As Action, Barcode As String)
        SyncLock SqliteLock
            If Not privateSqliteTrDic.ContainsKey(Barcode) Then
                Dim tr = GetSqliteConnection(Barcode).BeginTransaction()
                privateSqliteTrDic.AddOrUpdate(Barcode, tr, Function(key, oldValue) oldValue)
            End If
            action()
            SqliteTransQueue.Enqueue(1)
        End SyncLock
    End Sub
    Dim refreshThread As New Threading.Thread(AddressOf RefreshDisplayThread)
    Public Sub StartRefreshThread()
        If Not refreshThread.IsAlive Then
            refreshThread.IsBackground = True
            refreshThread.Start()
        End If
    End Sub

    Private Sub RefreshDisplayThread()
        While True
            Dim count As Integer = 0
            Dim startTime As DateTime = DateTime.Now

            ' 从队列中取出最多1000个元素，并执行refresh方法
            While count < 1000
                Dim item = 0
                If SqliteTransQueue.TryDequeue(item) Then
                    ' 执行refresh方法
                    count += 1
                Else
                    ' 检查是否超过1秒
                    Dim elapsedTime As TimeSpan = DateTime.Now - startTime
                    If elapsedTime.TotalSeconds >= 1 Then
                        Exit While
                    End If
                End If
            End While
            If count > 0 Then
                SyncLock SqliteLock
                    If privateSqliteDic.ContainsKey(Barcode) Then
                        Dim tr = privateSqliteTrDic(Barcode)
                        tr.Commit()
                        privateSqliteTrDic.TryRemove(Barcode, tr)
                    End If
                End SyncLock
            End If
        End While
    End Sub
    Public Sub Load_Settings()

        覆盖已有文件ToolStripMenuItem.Checked = My.Settings.LTFSWriter_OverwriteExist
        跳过符号链接ToolStripMenuItem.Checked = My.Settings.LTFSWriter_SkipSymlink
        显示文件数ToolStripMenuItem.Checked = My.Settings.LTFSWriter_ShowFileCount
        Select Case My.Settings.LTFSWriter_OnWriteFinished
            Case 0
                WA0ToolStripMenuItem.Checked = True
                WA1ToolStripMenuItem.Checked = False
                WA2ToolStripMenuItem.Checked = False
                WA3ToolStripMenuItem.Checked = False
            Case 1
                WA0ToolStripMenuItem.Checked = False
                WA1ToolStripMenuItem.Checked = True
                WA2ToolStripMenuItem.Checked = False
                WA3ToolStripMenuItem.Checked = False
            Case 2
                WA0ToolStripMenuItem.Checked = False
                WA1ToolStripMenuItem.Checked = False
                WA2ToolStripMenuItem.Checked = True
                WA3ToolStripMenuItem.Checked = False
            Case 3
                WA0ToolStripMenuItem.Checked = False
                WA1ToolStripMenuItem.Checked = False
                WA2ToolStripMenuItem.Checked = False
                WA3ToolStripMenuItem.Checked = True
        End Select
        APToolStripMenuItem.Checked = My.Settings.LTFSWriter_AutoFlush
        启用日志记录ToolStripMenuItem.Checked = My.Settings.LTFSWriter_LogEnabled
        总是更新数据区索引ToolStripMenuItem.Checked = My.Settings.LTFSWriter_ForceIndex
        计算校验ToolStripMenuItem.Checked = My.Settings.LTFSWriter_HashOnWriting
        异步校验CPU占用高ToolStripMenuItem.Checked = My.Settings.LTFSWriter_HashAsync
        预读文件数5ToolStripMenuItem.Text = $"{My.Resources.ResText_PFC}{My.Settings.LTFSWriter_PreLoadNum}"
        文件缓存32MiBToolStripMenuItem.Text = $"{My.Resources.ResText_FB}{IOManager.FormatSize(My.Settings.LTFSWriter_PreLoadBytes)}"
        禁用分区ToolStripMenuItem.Checked = DisablePartition
        速度下限ToolStripMenuItem.Text = $"{My.Resources.ResText_SMin}{My.Settings.LTFSWriter_AutoCleanDownLim} MiB/s"
        速度上限ToolStripMenuItem.Text = $"{My.Resources.ResText_SMax}{My.Settings.LTFSWriter_AutoCleanUpperLim} MiB/s"
        持续时间ToolStripMenuItem.Text = $"{My.Resources.ResText_STime}{My.Settings.LTFSWriter_AutoCleanTimeThreashould}s"
        去重SHA1ToolStripMenuItem.Checked = My.Settings.LTFSWriter_DeDupe
        Chart1.Titles(1).Text = My.Resources.ResText_SpeedBT
        Chart1.Titles(2).Text = My.Resources.ResText_FileRateBT
        TapeUtils.AllowPartition = Not DisablePartition
        CleanCycle = CleanCycle
        IndexWriteInterval = IndexWriteInterval
        CapacityRefreshInterval = CapacityRefreshInterval
    End Sub
    Public Sub Save_Settings()
        My.Settings.LTFSWriter_OverwriteExist = 覆盖已有文件ToolStripMenuItem.Checked
        My.Settings.LTFSWriter_SkipSymlink = 跳过符号链接ToolStripMenuItem.Checked
        If WA0ToolStripMenuItem.Checked Then
            My.Settings.LTFSWriter_OnWriteFinished = 0
        ElseIf WA1ToolStripMenuItem.Checked Then
            My.Settings.LTFSWriter_OnWriteFinished = 1
        ElseIf WA2ToolStripMenuItem.Checked Then
            My.Settings.LTFSWriter_OnWriteFinished = 2
        ElseIf WA3ToolStripMenuItem.Checked Then
            My.Settings.LTFSWriter_OnWriteFinished = 3
        End If
        My.Settings.LTFSWriter_AutoFlush = APToolStripMenuItem.Checked
        My.Settings.LTFSWriter_LogEnabled = 启用日志记录ToolStripMenuItem.Checked
        My.Settings.LTFSWriter_ForceIndex = 总是更新数据区索引ToolStripMenuItem.Checked
        My.Settings.LTFSWriter_HashOnWriting = 计算校验ToolStripMenuItem.Checked
        My.Settings.LTFSWriter_HashAsync = 异步校验CPU占用高ToolStripMenuItem.Checked
        My.Settings.Save()
    End Sub
    Private Text3 As String = "", Text5 As String = ""
    Private TextT3 As String = "", TextT5 As String = ""
    Private Sub Timer2_Tick(sender As Object, e As EventArgs) Handles Timer2.Tick
        ToolStripStatusLabel3.Text = Text3
        ToolStripStatusLabel3.ToolTipText = TextT3
        ToolStripStatusLabel5.Text = Text5
        ToolStripStatusLabel5.Text = Text5.Substring(0, Math.Min(Text5.Length, 20))
    End Sub
    Public Sub PrintMsg(s As String, Optional ByVal Warning As Boolean = False, Optional ByVal TooltipText As String = "", Optional ByVal LogOnly As Boolean = False, Optional ByVal ForceLog As Boolean = False)
        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} Warning:{Warning} printmsg:{s},TooltipText:{TooltipText} LogOnly:{LogOnly} ForceLog:{ForceLog}")
      
                      If ForceLog OrElse My.Settings.LTFSWriter_LogEnabled Then
                          Dim logType As String = "info"
                          If Warning Then logType = "warn"
                          Dim ExtraMsg As String = ""
                          If TooltipText <> "" Then
                              ExtraMsg = $"({TooltipText})"
                          End If
                          If Not IO.Directory.Exists(IO.Path.Combine(Application.StartupPath, "log")) Then
                              IO.Directory.CreateDirectory(IO.Path.Combine(Application.StartupPath, "log"))
                          End If
                          IO.File.AppendAllText(logFile, $"{vbCrLf}{Now.ToString("yyyy-MM-dd HH:mm:ss")} {logType}> {s} {ExtraMsg}")
                          If Warning Then
                              IO.File.AppendAllText(errorLogFile, $"{vbCrLf}{Now.ToString("yyyy-MM-dd HH:mm:ss")} {logType}> {s} {ExtraMsg}")
                      End If
                      End If
                    
                      If LogOnly Then Exit Sub
                      If TooltipText = "" Then TooltipText = s
                        Dim startTimestamp = DateTime.Now
             Me.Invoke(Sub()
                      If Not Warning Then
                          Text3 = s
                          TextT3 = TooltipText
                      Else
                          Text5 = s
                          TextT5 = TooltipText
                      End If
                      Dim duration1 As TimeSpan = DateTime.Now - startTimestamp
                      Metric.FileOperationDurationHistogram.WithLabels(Barcode, "print_msg_ui", "big").Observe(duration1.TotalMilliseconds)
                  End Sub)
    End Sub
    Public Shared LastGcTime As Datetime= DateTime.Now
    Public DataCompressionLogPage As TapeUtils.PageData
    Private Sub Timer1_Tick(sender As Object, e As EventArgs) Handles Timer1.Tick
        Static d_last As Long = 0
        Static t_last As Long = 0
        Static rwh_last As Long = 0
        Static rwt_last As Long = 0
        Try
            Dim i As Integer
            If False Then
                Dim logdata As Byte()
                Dim wcr, rcr, wh, wt, rh, rt As Long
                If DataCompressionLogPage Is Nothing Then
                    DataCompressionLogPage = New TapeUtils.PageData With {.Name = "Data Compression log page", .PageCode = &H1B}
                    DataCompressionLogPage.Items.Add(New TapeUtils.PageData.DataItem With {
                                              .Parent = DataCompressionLogPage,
                                              .Name = "Data Compression Parameter",
                                              .StartByte = 4,
                                              .BitOffset = 0,
                                              .TotalBits = 0,
                                              .DynamicParamCodeBitOffset = 0,
                                              .DynamicParamCodeStartByte = 0,
                                              .DynamicParamCodeTotalBits = 16,
                                              .DynamicParamLenBitOffset = 0,
                                              .DynamicParamLenStartByte = 3,
                                              .DynamicParamLenTotalBits = 8,
                                              .DynamicParamDataStartByte = 4,
                                              .EnumTranslator = New SerializableDictionary(Of Long, String),
                                              .DynamicParamType = New SerializableDictionary(Of Long, TapeUtils.PageData.DataItem.DataType),
                                              .Type = TapeUtils.PageData.DataItem.DataType.DynamicPage})
                    With DataCompressionLogPage.Items.Last.EnumTranslator
                        .Add(0, "Read compression ratio")
                        .Add(1, "Write compression ratio")
                        .Add(2, "Megabytes transferred to host")
                        .Add(3, "Bytes transferred to host")
                        .Add(4, "Megabytes read from tape")
                        .Add(5, "Bytes read from tape")
                        .Add(6, "Megabytes transferred from host")
                        .Add(7, "Bytes transferred from host")
                        .Add(8, "Megabytes written to tape")
                        .Add(9, "Bytes written to tape")
                        .Add(&H100, "Dara compression enabled")
                    End With
                    With DataCompressionLogPage.Items.Last.DynamicParamType
                        .Add(0, TapeUtils.PageData.DataItem.DataType.Int16)
                        .Add(1, TapeUtils.PageData.DataItem.DataType.Int16)
                        .Add(2, TapeUtils.PageData.DataItem.DataType.Int32)
                        .Add(3, TapeUtils.PageData.DataItem.DataType.Int32)
                        .Add(4, TapeUtils.PageData.DataItem.DataType.Int32)
                        .Add(5, TapeUtils.PageData.DataItem.DataType.Int32)
                        .Add(6, TapeUtils.PageData.DataItem.DataType.Int32)
                        .Add(7, TapeUtils.PageData.DataItem.DataType.Int32)
                        .Add(8, TapeUtils.PageData.DataItem.DataType.Int32)
                        .Add(9, TapeUtils.PageData.DataItem.DataType.Int32)
                        .Add(&H100, TapeUtils.PageData.DataItem.DataType.Boolean)
                    End With
                End If
                logdata = TapeUtils.LogSense(TapeDrive, &H1B, PageControl:=1)
                DataCompressionLogPage.RawData = logdata
                For Each it As TapeUtils.PageData.DataItem In DataCompressionLogPage.Items
                    Dim pi As Integer = 0
                    While pi < it.RawData.Length - 1
                        Dim nextPage As TapeUtils.PageData.DataItem.DynamicParamPage = TapeUtils.PageData.DataItem.DynamicParamPage.Next(it, pi)
                        Select Case nextPage.ParamCode
                            Case 0
                                rcr = 0
                                For j As Integer = 0 To nextPage.RawData.Length - 1
                                    rcr = rcr << 8
                                    rcr = rcr Or nextPage.RawData(j)
                                Next
                            Case 1
                                wcr = 0
                                For j As Integer = 0 To nextPage.RawData.Length - 1
                                    wcr = wcr << 8
                                    wcr = wcr Or nextPage.RawData(j)
                                Next
                            Case 3
                                rh = 0
                                For j As Integer = 0 To nextPage.RawData.Length - 1
                                    rh = rh << 8
                                    rh = rh Or nextPage.RawData(j)
                                Next
                            Case 5
                                rt = 0
                                For j As Integer = 0 To nextPage.RawData.Length - 1
                                    rt = rt << 8
                                    rt = rt Or nextPage.RawData(j)
                                Next
                            Case 7
                                wh = 0
                                For j As Integer = 0 To nextPage.RawData.Length - 1
                                    wh = wh << 8
                                    wh = wh Or nextPage.RawData(j)
                                Next
                            Case 9
                                wt = 0
                                For j As Integer = 0 To nextPage.RawData.Length - 1
                                    wt = wt << 8
                                    wt = wt Or nextPage.RawData(j)
                                Next
                        End Select

                        pi += nextPage.RawData.Length + it.DynamicParamDataStartByte
                    End While
                Next
                Dim rwhval As Long = rh + wh
                If rwhval = 0 Then rwh_last = 0
                If rwhval >= rwh_last Then
                    rwhdelta = rwhval - rwh_last
                End If
                rwh_last = rwhval
                Dim rwtval As Long = rt + wt
                If rwtval = 0 Then rwt_last = 0
                If rwtval >= rwt_last Then
                    rwtdelta = rwtval - rwt_last
                End If
                rwt_last = rwtval
                NativeSpeedHistory.Add(rwtdelta)
                While NativeSpeedHistory.Count > PMaxNum
                    NativeSpeedHistory.RemoveAt(0)
                End While
                i = 0
                Chart1.Series(2).Points.Clear()
                For Each val As Double In NativeSpeedHistory.GetRange(NativeSpeedHistory.Count - SMaxNum, SMaxNum)
                    Chart1.Series(2).Points.AddXY(i, val)
                    i += 1
                Next
            End If

            Dim pnow As Long = TotalBytesProcessed
            If pnow = 0 Then d_last = 0
            If pnow >= d_last Then
                ddelta = pnow - d_last
                d_last = pnow
            End If
            Dim tval As Long = TotalFilesProcessed
            If tval = 0 Then t_last = 0
            If tval >= t_last Then
                fdelta = tval - t_last
                t_last = tval
            End If


            SpeedHistory.Add(ddelta / 1048576)
            FileRateHistory.Add(fdelta)

            While SpeedHistory.Count > PMaxNum
                SpeedHistory.RemoveAt(0)
            End While
            While FileRateHistory.Count > PMaxNum
                FileRateHistory.RemoveAt(0)
            End While

            If APToolStripMenuItem.Checked AndAlso fdelta = 0 Then
                Dim FlushNow As Boolean = True
                For j As Integer = 1 To My.Settings.LTFSWriter_AutoCleanTimeThreashould
                    Dim n As Double = SpeedHistory(SpeedHistory.Count - j)
                    If n < My.Settings.LTFSWriter_AutoCleanDownLim Or n > My.Settings.LTFSWriter_AutoCleanUpperLim Then
                        FlushNow = False
                        Exit For
                    End If
                Next
                If CapReduceCount > 0 Then
                    ToolStripDropDownButton3.ToolTipText = $"{My.Resources.ResText_C0}{vbCrLf}{My.Resources.ResText_C1}{CapReduceCount}{vbCrLf}"
                    If CapReduceCount >= CleanCycle Then ToolStripDropDownButton3.ToolTipText &= $"{My.Resources.ResText_C2}{Clean_last.ToString("yyyy/MM/dd HH:mm:ss")}"
                End If
                Flush = FlushNow
                If FlushNow Then
                    CapReduceCount += 1
                    If CleanCycle > 0 AndAlso (CapReduceCount Mod CleanCycle = 0) Then
                        Flush = False
                        Clean = True
                    End If
                End If
            End If
            SyncLock OperationLock
                If AllowOperation Then CheckClean()
            End SyncLock

            i = 0
            Chart1.Series(0).Points.Clear()
            For Each val As Double In SpeedHistory.GetRange(SpeedHistory.Count - SMaxNum, SMaxNum)
                Chart1.Series(0).Points.AddXY(i, val)
                i += 1
            Next
            i = 0
            Chart1.Series(1).Points.Clear()
            For Each val As Double In FileRateHistory.GetRange(FileRateHistory.Count - SMaxNum, SMaxNum)
                Chart1.Series(1).Points.AddXY(i, val)
                i += 1
            Next
            Dim USize As Long = UnwrittenSize
            Dim UFile As Long = UnwrittenCount
            ToolStripStatusLabel4.Text = " "
            ToolStripStatusLabel4.Text &= $"{My.Resources.ResText_S0}{IOManager.FormatSize(ddelta)}/s"
            ToolStripStatusLabel4.Text &= $"  {My.Resources.ResText_S1}{IOManager.FormatSize(TotalBytesProcessed)}"
            If CurrentBytesProcessed > 0 Then ToolStripStatusLabel4.Text &= $"({IOManager.FormatSize(CurrentBytesProcessed)})"
            ToolStripStatusLabel4.Text &= $"|{TotalFilesProcessed}"
            If CurrentFilesProcessed > 0 Then ToolStripStatusLabel4.Text &= $"({CurrentFilesProcessed})"
            ToolStripStatusLabel4.Text &= $"  {My.Resources.ResText_S2}"
            If UFile > 0 AndAlso UFile >= CurrentFilesProcessed Then ToolStripStatusLabel4.Text &= $"[{UFile - CurrentFilesProcessed}/{UFile}]"
            ToolStripStatusLabel4.Text &= $"{ IOManager.FormatSize(Math.Max(0, USize - CurrentBytesProcessed))}/{IOManager.FormatSize(USize)}"
            ToolStripStatusLabel4.Text &= $"  {My.Resources.ResText_S3}{IOManager.FormatSize(TotalBytesUnindexed)}"
            ToolStripStatusLabel4.ToolTipText = ToolStripStatusLabel4.Text
            If USize > 0 AndAlso CurrentBytesProcessed >= 0 AndAlso CurrentBytesProcessed <= USize Then
                ToolStripProgressBar1.Value = CurrentBytesProcessed / USize * 10000
                ToolStripProgressBar1.ToolTipText = $"{My.Resources.ResText_S4}{IOManager.FormatSize(CurrentBytesProcessed)}/{IOManager.FormatSize(USize)}"
            End If
            Metric.TotalBytesProcessedGauge.WithLabels(Barcode).Set(CurrentBytesProcessed)
            Metric.CurrentBytesProcessedGauge.WithLabels(Barcode).Set(CurrentBytesProcessed)
            Metric.TotalFilesProcessedGauge.WithLabels(Barcode).Set(TotalFilesProcessed)
            Metric.CurrentFilesProcessedGauge.WithLabels(Barcode).Set(CurrentFilesProcessed)
            Metric.OperationProcessedGauge.WithLabels(Barcode, "UnwrittenCount").Set(UnwrittenCount)
            Metric.OperationProcessedGauge.WithLabels(Barcode, "UnwrittenSize").Set(UnwrittenSize)
            Metric.OperationProcessedGauge.WithLabels( "","async_sha_queue_count").Set(IOManager.CheckSumBlockwiseCalculator.TotalQueueCount)
            Text = GetLocInfo()
            Static GCCollectCounter As Integer
            GCCollectCounter += 1
            If GCCollectCounter >= 60 Then
                GC.Collect()
                GCCollectCounter = 0
            End If
        Catch ex As Exception
            PrintMsg(ex.ToString)
        End Try
    End Sub
    Public Class FileRecord
        Public ParentDirectory As ltfsindex.directory
        Public SourcePath As String
        Public File As ltfsindex.file
        Public Buffer As Byte() = Nothing
'        <JsonIgnore()>
'        Public Stream As New IO.MemoryStream
        Public IsMemoryFile As Boolean = False
        
        Private OperationLock As New Object
        Public Sub RemoveUnwritten()
            ParentDirectory.contents.UnwrittenFiles.Remove(File)
        End Sub
        Public Sub New()

        End Sub
        Public Sub New(Path As String, ParentDir As ltfsindex.directory)
            If Not Path.StartsWith("\\") Then Path = $"\\?\{Path}"
            ParentDirectory = ParentDir
            SourcePath = Path
            IsMemoryFile = True
            Dim finf As IO.FileInfo = New IO.FileInfo(SourcePath)
            File = New ltfsindex.file With {
                .name = finf.Name,
                .fileuid = -1,
                .length = finf.Length,
                .readonly = False,
                .openforwrite = False}
            File.SetXattr("diy.Computer", Environment.MachineName)
            File.SetXattr("diy.SourcePath", Path)

            With File
                Try
                    .creationtime = finf.CreationTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")
                Catch ex As Exception
                    .creationtime = Now.ToUniversalTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")
                End Try
                Try
                    .modifytime = finf.LastWriteTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")
                Catch ex As Exception
                    .modifytime = .creationtime
                End Try
                Try
                    .accesstime = finf.LastAccessTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")
                Catch ex As Exception
                    .accesstime = .creationtime
                End Try
                .changetime = .modifytime
                .backuptime = Now.ToUniversalTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")
                Try
                    If IO.File.Exists(Path & ".xattr") Then
                        Dim x As String = IO.File.ReadAllText(Path & ".xattr")
                        Dim xlist As List(Of ltfsindex.file.xattr) = ltfsindex.file.xattr.FromXMLList(x)
                        If .extendedattributes Is Nothing Then .extendedattributes = New List(Of ltfsindex.file.xattr)
                        .extendedattributes.AddRange(xlist)
                    End If
                Catch ex As Exception
                    MessageBox.Show(ex.ToString())
                End Try
            End With
            ParentDirectory.contents.UnwrittenFiles.Add(File)
        End Sub
        Public Sub New(Name As String, Length As Long, fullpath As String, ParentDir As ltfsindex.directory)
             ParentDirectory = ParentDir
            File = New ltfsindex.file With {
                .name = Name,
                .fullpath = fullpath,
                .fileuid = -1,
                .length = Length,
                .readonly = False,
                .openforwrite = False}
            File.SetXattr("diy.Computer", Environment.MachineName)
'            File.SetXattr("diy.RelativePath", RelativePath)

            With File
                Try
                    .creationtime = Now.ToUniversalTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")
                Catch ex As Exception
                    
                End Try
                Try
                    .modifytime = .creationtime
                Catch ex As Exception
                    .modifytime = .creationtime
                End Try
                Try
                    .accesstime = .creationtime
                Catch ex As Exception
                    .accesstime = .creationtime
                End Try
                .changetime = .modifytime
                .backuptime = Now.ToUniversalTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")
            End With
            If Not ParentDirectory Is Nothing Then
                ParentDirectory.contents.UnwrittenFiles.Add(File)
            End If
        End Sub
        Public fs As IO.FileStream
        Public fsB As IO.BufferedStream
        Public fsPreRead As IO.FileStream
        Public PreReadOffset As Long = 0
        Public PreReadByteCount As Long = 0
        Public PreReadOffsetLock As New Object
        Public Event PreReadFinished()
        'Public ReadOnly Property PreReadEnabled
        '    Get
        '        Return (My.Settings.LTFSWriter_PreLoadNum = 0)
        '    End Get
        'End Property
        Const PreReadBufferSize As Long = 16777216
        Const PreReadBlockSize As Long = 8388608
        Public PreReadBuffer As Byte() = Nothing
        Public Sub PreReadThread()
            If PreReadBuffer Is Nothing Then ReDim PreReadBuffer(PreReadBufferSize * 2 - 1)
            While True
                if fsPreRead is Nothing Then Exit Sub
                Dim rBytes As Long = fsPreRead.Read(PreReadBuffer, PreReadByteCount Mod PreReadBufferSize, PreReadBlockSize)
                If rBytes = 0 Then Exit While
                Threading.Interlocked.Add(PreReadByteCount, rBytes)
                While PreReadByteCount - PreReadOffset >= PreReadBufferSize
                    Threading.Thread.Sleep(1)
                End While
            End While
            RaiseEvent PreReadFinished()
        End Sub
        Public Function Open(Optional BufferSize As Integer = 16777216) As Integer
            SyncLock OperationLock
                While True
                    Try
                        If fs IsNot Nothing Then Return 1
                        fs = New IO.FileStream(SourcePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read, BufferSize, True)
                        fsPreRead = New IO.FileStream(SourcePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read, BufferSize, True)
                 If fsPreRead Is Nothing Then
                  Metric.ErrorGauge.WithLabels($"fsPreRead is nothing", SourcePath)
                  Else
                   Metric.PreReadCounter.WithLabels("").Inc()
                 End If
                        Task.Run(Sub() PreReadThread())
                        fsB = New IO.BufferedStream(fs, PreReadBufferSize)
                        Exit While
                    Catch ex As Exception
                        Select Case MessageBox.Show($"{My.Resources.ResText_WErr }{vbCrLf}{ex.ToString}", My.Resources.ResText_Warning, MessageBoxButtons.AbortRetryIgnore)
                            Case DialogResult.Abort
                                Return 3
                            Case DialogResult.Retry

                            Case DialogResult.Ignore
                                Return 5
                        End Select
                    End Try
                End While
            End SyncLock
            Return 1
        End Function
        Public Function BeginOpen(Optional BufferSize As Integer = 0, Optional ByVal BlockSize As Integer = 524288) As Integer
            While True
                Try
                    If File.length <= BlockSize Then
                        If Buffer IsNot Nothing Then Return 1
                        Buffer = IO.File.ReadAllBytes(SourcePath)
                        Return 1
                    End If
                    SyncLock OperationLock
                        If fs IsNot Nothing Then Return 1
                    End SyncLock
                    If BufferSize = 0 Then BufferSize = My.Settings.LTFSWriter_PreLoadBytes
                    If BufferSize = 0 Then BufferSize = 524288
                    Exit While
                Catch ex As Exception
                    Select Case MessageBox.Show($"{My.Resources.ResText_WErr}{vbCrLf}{ex.ToString}", My.Resources.ResText_Warning, MessageBoxButtons.AbortRetryIgnore)
                        Case DialogResult.Abort
                            Return 3
                        Case DialogResult.Retry

                        Case DialogResult.Ignore
                            Return 5
                    End Select
                End Try
            End While
            Task.Run(Sub()
                         While True
                             Try
                                 SyncLock OperationLock
                                     If fs IsNot Nothing Then Exit Sub
                                     fs = New IO.FileStream(SourcePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read, BufferSize, True)
                                     'If PreReadEnabled Then
                                     fsPreRead = New IO.FileStream(SourcePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read, BufferSize, True)
                                     Task.Run(Sub() PreReadThread())
                                     'Dim stackTrace As New StackTrace()
                                     'Else
                                     fsB = New IO.BufferedStream(fs, PreReadBufferSize)
                                     'End If
                                     Exit While
                                 End SyncLock
                             Catch ex As Exception
                                     Select Case MessageBox.Show($"{My.Resources.ResText_WErr }{vbCrLf}{ex.ToString}", My.Resources.ResText_Warning, MessageBoxButtons.AbortRetryIgnore)
                                         Case DialogResult.Abort
                                             Exit While
                                         Case DialogResult.Retry

                                         Case DialogResult.Ignore
                                             Exit While
                                     End Select
                                 End Try
                         End While
                     End Sub)
            Return 1
        End Function
        Public Function Read(array As Byte(), offset As Integer, count As Integer) As Integer
            'If PreReadEnabled Then
            SyncLock PreReadOffsetLock
                PreReadOffset = Math.Max(fs.Position, PreReadOffset)
            End SyncLock
            'Return fs.Read(array, offset, count)
            'Else
            Return fsB.Read(array, offset, count)
            'End If
        End Function
        Public Sub Close()
            SyncLock OperationLock
                If fsB IsNot Nothing Then
                    fsB.Close()
                    fsB.Dispose()
                    fsB = Nothing
                End If
                fs.Close()
                fs.Dispose()
                fs = Nothing
                If fsPreRead IsNot Nothing Then
                    fsPreRead.Close()
                    fsPreRead.Dispose()
                    fsPreRead = Nothing
                End If
            End SyncLock
        End Sub
        Public Sub CloseAsync()
            Task.Run(Sub()
                         SyncLock OperationLock
                             fs.Close()
                             fs.Dispose()
                             fs = Nothing
                             'If PreReadEnabled Then
                             If fsPreRead IsNot Nothing Then
                                 fsPreRead.Close()
                                 fsPreRead.Dispose()
                                 fsPreRead = Nothing
                             End If
                             'Else
                             If fsB IsNot Nothing Then
                                 fsB.Close()
                                 fsB.Dispose()
                                 fsB = Nothing
                             End If
                             ' End If
                         End SyncLock
                     End Sub)
        End Sub
        Public Function ReadAllBytes() As Byte()
            If Buffer IsNot Nothing AndAlso Buffer.Length > 0 Then
                Dim result As Byte() = Buffer
                Buffer = Nothing
                Return result
            Else
                Return IO.File.ReadAllBytes(SourcePath)
            End If
        End Function
    End Class
    Public Class IntLock
        Public Property Value As Integer = 0
        Public Sub Inc()
            SyncLock Me
                Threading.Interlocked.Increment(Value)
            End SyncLock
        End Sub
        Public Sub Dec()
            SyncLock Me
                Threading.Interlocked.Decrement(Value)
            End SyncLock
        End Sub
        Public Shared Widening Operator CType(n As Integer) As IntLock
            Return New IntLock With {.Value = n}
        End Operator
        Public Shared Operator >(a As IntLock, b As Integer) As Boolean
            Return a.Value > b
        End Operator
        Public Shared Operator <(a As IntLock, b As Integer) As Boolean
            Return a.Value < b
        End Operator
    End Class
    Public UFReadCount As IntLock = 0
    Public UnwrittenFiles As New List(Of FileRecord)
    Public Property UnwrittenSizeOverrideValue As ULong = 0
    Public ReadOnly Property UnwrittenSize
        Get
            If UnwrittenSizeOverrideValue > 0 Then Return UnwrittenSizeOverrideValue
            If UnwrittenFiles Is Nothing Then Return 0
            Dim result As Long = 0

            UFReadCount.Inc()
            If UnwrittenFiles.Count > 0 Then
                For Each fr As FileRecord In UnwrittenFiles
                    result += fr.File.length
                Next
            End If
            UFReadCount.Dec()
            Return result
        End Get
    End Property
    Public Property UnwrittenCountOverwriteValue As ULong = 0
    Public ReadOnly Property UnwrittenCount
        Get
            If UnwrittenCountOverwriteValue > 0 Then Return UnwrittenCountOverwriteValue
            Return UnwrittenFiles.Count
        End Get
    End Property
    Public Dim LastRefresh As Date = Now
    Private Sub LTFSWriter_Load(sender As Object, e As EventArgs) Handles MyBase.Load
        StartRefreshThread()
        FileDroper = New FileDropHandler(ListView1)
        Load_Settings()
        If OfflineMode Then Exit Sub
        Try

'            回滚ToolStripMenuItem_Click(sender, e)
            读取索引ToolStripMenuItem_Click(sender, e)
        Catch ex As Exception
            Console.WriteLine($"{ex.Message} {ex.StackTrace}")
            PrintMsg(My.Resources.ResText_ErrP)
        End Try

    End Sub
    Private Sub LTFSWriter_Closing(sender As Object, e As CancelEventArgs) Handles Me.Closing
        Static ForceCloseCount As Integer = 0
        e.Cancel = False
        If Not AllowOperation Then
            If ForceCloseCount < 3 Then
                MessageBox.Show(My.Resources.ResText_X0)
            Else
                If MessageBox.Show(My.Resources.ResText_X1, My.Resources.ResText_Warning, MessageBoxButtons.OKCancel) = DialogResult.OK Then
                    Save_Settings()
                    e.Cancel = False
                    End
                    Exit Sub
                End If
            End If
            ForceCloseCount += 1
            e.Cancel = True
            Exit Sub
        End If
        If TotalBytesUnindexed > 0 Then
            If MessageBox.Show(My.Resources.ResText_X2, My.Resources.ResText_Warning, MessageBoxButtons.YesNo) = DialogResult.No Then
                e.Cancel = True
                Exit Sub
            Else
                Save_Settings()
                e.Cancel = False
                UFReadCount.Value = 0
                Exit Sub
            End If
        End If
        If ExtraPartitionCount > 0 AndAlso Modified Then
            If MessageBox.Show(My.Resources.ResText_X3, My.Resources.ResText_Warning, MessageBoxButtons.YesNo) = DialogResult.No Then
                e.Cancel = True
            Else
                Save_Settings()
                e.Cancel = False
                UFReadCount.Value = 0
            End If
        End If
        Save_Settings()
    End Sub
    Public Function GetLocInfo() As String
        If schema Is Nothing Then Return $"{My.Resources.ResText_NIndex} - {My.Application.Info.ProductName} {My.Application.Info.Version.ToString(3)}{My.Settings.License}"
        Dim info As String = $"{Barcode.TrimEnd()} ".TrimStart()
        If TapeDrive <> "" Then info &= $"[{TapeDrive}] "
        Try
            SyncLock schema
                info &= $"{My.Resources.ResText_Index}{schema.generationnumber} - {My.Resources.ResText_Partition}{schema.location.partition} - {My.Resources.ResText_Block}{schema.location.startblock}"
                If schema.previousgenerationlocation IsNot Nothing Then
                    If schema.previousgenerationlocation.startblock > 0 Then info &= $" ({My.Resources.ResText_Previous}:{My.Resources.ResText_Partition}{schema.previousgenerationlocation.partition} - {My.Resources.ResText_Block}{schema.previousgenerationlocation.startblock})"
                End If
            End SyncLock
            If CurrentHeight > 0 Then info &= $" {My.Resources.ResText_WritePointer}{CurrentHeight}"
            If Modified Then info &= "*"
            info &= $" - {My.Application.Info.ProductName} {My.Application.Info.Version.ToString(3)}{My.Settings.License}"
        Catch ex As Exception
            PrintMsg(My.Resources.ResText_RPosErr)
        End Try
        Return info
    End Function
    Public Function GetProgressImage(ByVal value As Integer, ByVal maximum As Integer, ByVal color As Color) As Bitmap
        If maximum = 0 Then Return Nothing
        Dim result As New Bitmap(100, 1)
        Dim bd As Imaging.BitmapData = result.LockBits(New Rectangle(0, 0, 100, 1), Imaging.ImageLockMode.ReadWrite, Imaging.PixelFormat.Format24bppRgb)
        Dim b(bd.Stride - 1) As Byte
        value = Math.Max(0, value)
        value = Math.Min(value, maximum)
        For i As Integer = 0 To b.Length - 1
            b(i) = 255
        Next
        For i As Integer = 0 To value / maximum * 100
            b(i * 3 + 0) = color.B
            b(i * 3 + 1) = color.G
            b(i * 3 + 2) = color.R
        Next
        Marshal.Copy(b, 0, bd.Scan0, b.Length)
        result.UnlockBits(bd)
        Return result
    End Function
    Public MaxCapacity As Long = 0
    Public CapacityLogPage As TapeUtils.PageData

    Public Sub RefreshCapacity()
        Dim logdata As Byte()
        logdata = TapeUtils.LogSense(TapeDrive, &H31, PageControl:=1)
        Invoke(Sub()
                   Try

                       If CapacityLogPage Is Nothing Then
                           CapacityLogPage = New TapeUtils.PageData With {.Name = "Tape Capacity log page", .PageCode = &H31, .RawData = logdata}
                           CapacityLogPage.Items.Add(New TapeUtils.PageData.DataItem With {
                                           .Parent = CapacityLogPage,
                                           .Name = "Data Compression Parameter",
                                           .StartByte = 4,
                                           .BitOffset = 0,
                                           .TotalBits = 0,
                                           .DynamicParamCodeBitOffset = 0,
                                           .DynamicParamCodeStartByte = 0,
                                           .DynamicParamCodeTotalBits = 16,
                                           .DynamicParamLenBitOffset = 0,
                                           .DynamicParamLenStartByte = 3,
                                           .DynamicParamLenTotalBits = 8,
                                           .DynamicParamDataStartByte = 4,
                                           .EnumTranslator = New SerializableDictionary(Of Long, String),
                                           .DynamicParamType = New SerializableDictionary(Of Long, TapeUtils.PageData.DataItem.DataType),
                                           .Type = TapeUtils.PageData.DataItem.DataType.DynamicPage})
                           With CapacityLogPage.Items.Last.EnumTranslator
                               .Add(1, "Partition 0 Remaining Capacity")
                               .Add(2, "Partition 1 Remaining Capacity")
                               .Add(3, "Partition 0 Maximum Capacity")
                               .Add(4, "Partition 1 Maximum Capacity")
                               .Add(5, "Partition 2 Remaining Capacity")
                               .Add(6, "Partition 3 Remaining Capacity")
                               .Add(7, "Partition 2 Maximum Capacity")
                               .Add(8, "Partition 3 Maximum Capacity")
                           End With
                           With CapacityLogPage.Items.Last.DynamicParamType
                               .Add(1, TapeUtils.PageData.DataItem.DataType.Int32)
                               .Add(2, TapeUtils.PageData.DataItem.DataType.Int32)
                               .Add(3, TapeUtils.PageData.DataItem.DataType.Int32)
                               .Add(4, TapeUtils.PageData.DataItem.DataType.Int32)
                               .Add(5, TapeUtils.PageData.DataItem.DataType.Int32)
                               .Add(6, TapeUtils.PageData.DataItem.DataType.Int32)
                               .Add(7, TapeUtils.PageData.DataItem.DataType.Int32)
                               .Add(8, TapeUtils.PageData.DataItem.DataType.Int32)
                           End With
                       End If

                       CapacityLogPage.RawData = logdata
                       Dim cap0, cap1, max0, max1 As Long
                       For Each it As TapeUtils.PageData.DataItem In CapacityLogPage.Items
                           Dim i As Integer = 0
                           While i < it.RawData.Length - 1
                               Dim nextPage As TapeUtils.PageData.DataItem.DynamicParamPage = TapeUtils.PageData.DataItem.DynamicParamPage.Next(it, i)
                               Select Case nextPage.ParamCode
                                   Case 1
                                       cap0 = 0
                                       For j As Integer = 0 To nextPage.RawData.Length - 1
                                           cap0 = cap0 << 8
                                           cap0 = cap0 Or nextPage.RawData(j)
                                       Next
                                   Case 2
                                       cap1 = 0
                                       For j As Integer = 0 To nextPage.RawData.Length - 1
                                           cap1 = cap1 << 8
                                           cap1 = cap1 Or nextPage.RawData(j)
                                       Next
                                   Case 3
                                       max0 = 0
                                       For j As Integer = 0 To nextPage.RawData.Length - 1
                                           max0 = max0 << 8
                                           max0 = max0 Or nextPage.RawData(j)
                                       Next
                                   Case 4
                                       max1 = 0
                                       For j As Integer = 0 To nextPage.RawData.Length - 1
                                           max1 = max1 << 8
                                           max1 = max1 Or nextPage.RawData(j)
                                       Next
                               End Select

                               i += nextPage.RawData.Length + it.DynamicParamDataStartByte
                           End While
                       Next

                       'cap0 = TapeUtils.MAMAttribute.FromTapeDrive(TapeDrive, 0, 0, 0).AsNumeric
                       Dim loss As Long

                       If My.Settings.LTFSWriter_ShowLoss Then
                           Dim CMInfo As New TapeUtils.CMParser(TapeDrive)
                           Dim nLossDS As Long = 0
                           Dim DataSize As New List(Of Long)
                           If CMInfo.CartridgeMfgData.CartridgeTypeAbbr = "CU" Then Exit Try
                           Dim StartBlock As Integer = 0
                           Dim CurrSize As Long = 0
                           Dim gw As Boolean = False
                           For wn As Integer = 0 To CMInfo.a_NWraps - 1
                               Dim StartBlockStr As String = StartBlock.ToString()
                               If CMInfo.TapeDirectoryData.CapacityLoss(wn) = -1 Or CMInfo.TapeDirectoryData.CapacityLoss(wn) = -3 Then StartBlockStr = ""
                               Dim EndBlock As Integer = StartBlock + CMInfo.TapeDirectoryData.WrapEntryInfo(wn).RecCount + CMInfo.TapeDirectoryData.WrapEntryInfo(wn).FileMarkCount - 1
                               If CMInfo.TapeDirectoryData.CapacityLoss(wn) = -2 Then EndBlock += 1
                               StartBlock += CMInfo.TapeDirectoryData.WrapEntryInfo(wn).RecCount + CMInfo.TapeDirectoryData.WrapEntryInfo(wn).FileMarkCount
                               If CMInfo.TapeDirectoryData.CapacityLoss(wn) >= 0 Then
                                   nLossDS += Math.Max(0, CMInfo.a_SetsPerWrap - CMInfo.TapeDirectoryData.DatasetsOnWrapData(wn).Data)
                                   CurrSize += CMInfo.TapeDirectoryData.DatasetsOnWrapData(wn).Data
                               ElseIf CMInfo.TapeDirectoryData.CapacityLoss(wn) = -1 Then
                                   StartBlock = 0
                               ElseIf CMInfo.TapeDirectoryData.CapacityLoss(wn) = -2 Then
                                   CurrSize += CMInfo.TapeDirectoryData.DatasetsOnWrapData(wn).Data
                               ElseIf CMInfo.TapeDirectoryData.CapacityLoss(wn) = -3 Then
                                   StartBlock = 0
                                   If gw Then
                                       DataSize.Add(CurrSize)
                                       CurrSize = 0
                                       gw = False
                                   Else
                                       gw = True
                                   End If
                               End If
                           Next
                           loss = nLossDS * CMInfo.CartridgeMfgData.KB_PER_DATASET * 1000

                       End If
                       Dim lshbits As Byte = 20

                       'DAT Unit in KB
                       If max0 > 20 * 1024 * 1024 Then lshbits = 10

                       If ExtraPartitionCount > 0 Then
                           MaxCapacity = max1
                           If MaxCapacity = 0 Then MaxCapacity = TapeUtils.MAMAttribute.FromTapeDrive(TapeDrive, 0, 1, 1).AsNumeric
                           'cap1 = TapeUtils.MAMAttribute.FromTapeDrive(TapeDrive, 0, 0, 1).AsNumeric
                           ToolStripStatusLabel2.Text = $"{My.Resources.ResText_CapRem} P0:{IOManager.FormatSize(cap0 << lshbits)} P1:{IOManager.FormatSize(cap1 << lshbits)}"
                           ToolStripStatusLabel2.ToolTipText = $"{My.Resources.ResText_CapRem} P0:{LTFSConfigurator.ReduceDataUnit(cap0 >> (20 - lshbits))} P1:{LTFSConfigurator.ReduceDataUnit(cap1 >> (20 - lshbits))}"
                           If cap1 >= 4096 Then
                               ToolStripStatusLabel2.BackgroundImage = GetProgressImage(MaxCapacity - cap1, MaxCapacity, Color.FromArgb(121, 196, 232))
                           Else
                               ToolStripStatusLabel2.BackgroundImage = GetProgressImage(MaxCapacity - cap1, MaxCapacity, Color.FromArgb(255, 127, 127))
                           End If
                       Else
                           MaxCapacity = max0
                           If MaxCapacity = 0 Then MaxCapacity = TapeUtils.MAMAttribute.FromTapeDrive(TapeDrive, 0, 1, 0).AsNumeric
                           ToolStripStatusLabel2.Text = $"{My.Resources.ResText_CapRem} P0:{IOManager.FormatSize(cap0 << lshbits)}"
                           ToolStripStatusLabel2.ToolTipText = $"{My.Resources.ResText_CapRem} P0:{LTFSConfigurator.ReduceDataUnit(cap0 >> (20 - lshbits))}"
                           If cap0 >= 4096 Then
                               ToolStripStatusLabel2.BackgroundImage = GetProgressImage(MaxCapacity - cap0, MaxCapacity, Color.FromArgb(121, 196, 232))
                           Else
                               ToolStripStatusLabel2.BackgroundImage = GetProgressImage(MaxCapacity - cap0, MaxCapacity, Color.FromArgb(255, 127, 127))
                           End If
                       End If
                       If My.Settings.LTFSWriter_ShowLoss Then
                           ToolStripStatusLabel2.Text &= $" Loss:{IOManager.FormatSize(loss)}"
                           ToolStripStatusLabel2.ToolTipText &= $" Loss:{IOManager.FormatSize(loss)}"
                       End If
                       LastRefresh = Now
                   Catch ex As Exception
                       PrintMsg(My.Resources.ResText_RCErr)
                   End Try
               End Sub)

    End Sub
    Public Function GetCapacityMegaBytes() As Long
        If ExtraPartitionCount > 0 Then
            Return TapeUtils.MAMAttribute.FromTapeDrive(TapeDrive, 0, 0, 1).AsNumeric
        Else
            Return TapeUtils.MAMAttribute.FromTapeDrive(TapeDrive, 0, 0, 0).AsNumeric
        End If
    End Function
    Public Sub RefreshDisplay()
        Invoke(
            Sub()
                If IsSqliteTreeView Then
                    TreeView1.Nodes.Clear()
                    Dim root As New TreeNode

                    root.Text = Barcode
                    root.Tag = New ltfsindex.directory With {.name = Barcode, .readonly = False,
                  .creationtime = plabel.formattime, .changetime = .creationtime,
                  .accesstime = .creationtime, .modifytime = .creationtime, .backuptime = .creationtime, .fileuid = 1, .contents = New ltfsindex.contentsDef()}
                    root.ImageIndex = 0
                    TreeView1.Nodes.Add(root)

                    Dim fileAndDir = DirProvider.ReadDirWithWhere($"ParentPath= '' and isDirectory=1", GetSqliteConnection(Barcode))
                    For Each kv As DictionaryEntry In fileAndDir
                        Dim t As New TreeNode
                        Dim d As ltfsindex.directory = kv.Value

                        t.Text = d.name
                        t.Tag = d
                        t.ImageIndex = 1
                        t.SelectedImageIndex = 1
                        t.StateImageIndex = 1
                        root.Nodes.Add(t)
                    Next
                    TreeView1.TopNode.Expand()
                    Try
                        Text = GetLocInfo()
                        ToolStripStatusLabel4.Text = $"{My.Resources.ResText_DNW} {IOManager.FormatSize(UnwrittenSize)}"
                        ToolStripStatusLabel4.ToolTipText = ToolStripStatusLabel4.Text
                    Catch ex As Exception
                        PrintMsg(My.Resources.ResText_RDErr)
                    End Try
                    Return
                End If
                If My.Settings.LTFSWriter_ShowFileCount Then schema._directory(0).DeepRefreshCount()
                If schema Is Nothing Then Exit Sub
                Try
                    Dim old_select As ltfsindex.directory = Nothing
                    Dim old_select_path As String = ""
                    Dim new_select As TreeNode = Nothing
                    Dim IterDirectory As Action(Of ltfsindex.directory, TreeNode, Integer) =
                        Sub(dir As ltfsindex.directory, node As TreeNode, ByVal MaxDepth As Integer)
                            Dim NodeExpand As Action =
                                   Sub()
                                       'PrintMsg(dir.name, LogOnly:=True, ForceLog:=True)
                            SyncLock dir.contents._directory
                                For Each d As ltfsindex.directory In dir.contents._directory
                                    Dim t As New TreeNode
                                    If My.Settings.LTFSWriter_ShowFileCount Then
                                        If d.TotalFilesUnwritten = 0 Then
                                            t.Text = $"{d.TotalFiles.ToString.PadRight(6)}| {d.name}"
                                        Else
                                            t.Text = $"{$"{d.TotalFiles.ToString}+{d.TotalFilesUnwritten.ToString}".PadRight(6)}| {d.name}"
                                        End If
                                    Else
                                        t.Text = d.name
                                    End If
                                    t.Tag = d
                                    t.ImageIndex = 1
                                    t.SelectedImageIndex = 1
                                    t.StateImageIndex = 1
                                    node.Nodes.Add(t)
                                               IterDirectory(d, t, MaxDepth - 1)
                                    If old_select Is d Then
                                        new_select = t
                                    End If
                                Next
                                'Compressed Dir
                                For Each f As ltfsindex.file In dir.contents._file
                                    Dim s As String = f.GetXAttr("ltfscopygui.archive")
                                    If s IsNot Nothing AndAlso s.ToLower = "true" Then
                                        Dim t As New TreeNode
                                        t.Text = $"*{f.name}"
                                        t.Tag = f
                                        t.ImageIndex = 3
                                        t.SelectedImageIndex = 3
                                        t.StateImageIndex = 3
                                        node.Nodes.Add(t)
                                    End If
                                Next
                            End SyncLock
                                   End Sub
                            Dim tvNodeExpand As New TreeViewEventHandler(
                                Sub(sender As Object, e As TreeViewEventArgs)
                                    If e.Node IsNot node.Parent Then Exit Sub
                                    If node.Nodes IsNot Nothing AndAlso node.Nodes.Count > 0 Then Exit Sub
                                    NodeExpand()
                                    RemoveHandler TreeView1.AfterExpand, tvNodeExpand
                                End Sub)
                            Dim tvNodeSelect As New TreeViewEventHandler(
                                Sub(sender As Object, e As TreeViewEventArgs)
                                    If e.Node IsNot node Then Exit Sub
                                    If node.Nodes IsNot Nothing AndAlso node.Nodes.Count > 0 Then Exit Sub
                                    NodeExpand()
                                    RemoveHandler TreeView1.AfterSelect, tvNodeSelect
                                End Sub)
                            Dim isParentOfOldSelect As Boolean = False
                            If MaxDepth = 0 AndAlso Not old_select_path.StartsWith(node.FullPath) Then
                                AddHandler TreeView1.AfterExpand, tvNodeExpand
                                AddHandler TreeView1.AfterSelect, tvNodeSelect
                                MaxDepth = 2
                                Exit Sub
                            Else
                                NodeExpand()
                            End If
                        End Sub
                    If TreeView1.SelectedNode IsNot Nothing Then
                        If TreeView1.SelectedNode.Tag IsNot Nothing Then
                            If TypeOf TreeView1.SelectedNode.Tag Is ltfsindex.directory Then
                                old_select = TreeView1.SelectedNode.Tag
                                old_select_path = TreeView1.SelectedNode.FullPath
                            End If
                        End If
                    End If
                    If old_select Is Nothing And ListView1.Tag IsNot Nothing Then
                        old_select = ListView1.Tag
                        old_select_path = TreeView1.TopNode.FullPath
                    End If
                    TreeView1.Nodes.Clear()
                    SyncLock schema._directory
                        For Each d As ltfsindex.directory In schema._directory
                            Dim t As New TreeNode
                            t.Text = d.name
                            t.Tag = d
                            t.ImageIndex = 0
                            TreeView1.Nodes.Add(t)
                            IterDirectory(d, t, 2)
                        Next
                    End SyncLock
                    TreeView1.TopNode.Expand()
                    If new_select IsNot Nothing Then
                        TreeView1.SelectedNode = new_select
                        new_select.Expand()
                    Else
                        TreeView1.SelectedNode = TreeView1.TopNode
                    End If
                Catch ex As Exception

                End Try
                Try
                    Text = GetLocInfo()
                    ToolStripStatusLabel4.Text = $"{My.Resources.ResText_DNW} {IOManager.FormatSize(UnwrittenSize)}"
                    ToolStripStatusLabel4.ToolTipText = ToolStripStatusLabel4.Text
                Catch ex As Exception
                    PrintMsg(My.Resources.ResText_RDErr)
                End Try

            End Sub)
    End Sub
    Private Sub ToolStripStatusLabel2_Click(sender As Object, e As EventArgs) Handles ToolStripStatusLabel2.Click
        Try
            If True OrElse AllowOperation Then
                Task.Run(Sub()
                RefreshCapacity()
                PrintMsg(My.Resources.ResText_CRef)
                         End Sub)
            Else
                LastRefresh = Now - New TimeSpan(0, 0, CapacityRefreshInterval)
            End If
        Catch ex As Exception
            PrintMsg(My.Resources.ResText_CRefErr)
        End Try

    End Sub
    Public Sub LockGUI(Optional ByVal Lock As Boolean = True)
        Invoke(Sub()
                   SyncLock OperationLock
                       AllowOperation = Not Lock
                       'MenuStrip1.Enabled = AllowOperation
                       ContextMenuStrip1.Enabled = AllowOperation
                       For Each Items As ToolStripMenuItem In MenuStrip1.Items
                           For Each SubItem In Items.DropDownItems
                               If TypeOf (SubItem) Is ToolStripDropDownItem Then
                                   CType(SubItem, ToolStripDropDownItem).Enabled = AllowOperation
                               End If
                           Next
                       Next
                       自动化ToolStripMenuItem1.Enabled = True
                       ToolStrip1.Enabled = AllowOperation
                       ContextMenuStrip3.Enabled = AllowOperation
                   End SyncLock
               End Sub)
    End Sub
    Public Sub TriggerTreeView1Event()
        If TreeView1.SelectedNode IsNot Nothing AndAlso TreeView1.SelectedNode.Tag IsNot Nothing Then
            Try
                If TypeOf (TreeView1.SelectedNode.Tag) Is ltfsindex.directory Then
                    If TreeView1.SelectedNode.Parent IsNot Nothing Then
                        压缩索引ToolStripMenuItem.Enabled = True
                        删除ToolStripMenuItem.Enabled = True
                    Else
                        压缩索引ToolStripMenuItem.Enabled = False
                        删除ToolStripMenuItem.Enabled = False
                    End If
                    压缩索引ToolStripMenuItem.Visible = True
                    解压索引ToolStripMenuItem.Visible = False
                    提取ToolStripMenuItem1.Enabled = True
                    校验ToolStripMenuItem1.Enabled = True
                    重命名ToolStripMenuItem.Enabled = True
                    统计ToolStripMenuItem.Enabled = True
                    Dim d As ltfsindex.directory = TreeView1.SelectedNode.Tag
                    ListView1.Items.Clear()
                    ListView1.Tag = d
                    SyncLock d.contents._file
                        Dim files = d.contents._file
                        If IsSqliteTreeView Then
                            If IsSqliteTreeView Then
                                files = DirProvider.QueryFileWithWhere($"ParentPath='{d.fullpath}' and isdirectory=0", GetSqliteConnection(Barcode))
                            End If
                        End If
                        For Each f As ltfsindex.file In files
                            Dim li As New ListViewItem
                            li.Tag = f
                            li.Text = f.name
                            li.ImageIndex = 2
                            li.StateImageIndex = 2
                            Dim s(15) As String
                            s(0) = f.length
                            s(1) = f.creationtime
                            s(2) = f.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True)
                            s(15) = f.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True)
                            s(3) = f.fileuid
                            s(4) = f.openforwrite
                            s(5) = f.readonly
                            s(6) = f.changetime
                            s(7) = f.modifytime
                            s(8) = f.accesstime
                            s(9) = f.backuptime
                            If f.tag IsNot Nothing Then
                                s(10) = f.tag.ToString()
                            Else
                                s(10) = ""
                            End If
                            If f.extentinfo IsNot Nothing Then
                                If f.extentinfo.Count > 0 Then
                                    Try
                                        s(11) = (f.extentinfo(0).startblock.ToString())
                                        s(12) = (f.extentinfo(0).partition.ToString())
                                    Catch ex As Exception
                                        s(11) = ("-")
                                        s(12) = ("-")
                                    End Try
                                End If
                            Else
                                s(11) = ("-")
                                s(12) = ("-")
                            End If
                            s(13) = IOManager.FormatSize(f.length)
                            If f.WrittenBytes > 0 Then
                                s(14) = (IOManager.FormatSize(f.WrittenBytes))
                            Else
                                s(14) = ("-")
                            End If
                            For Each t As String In s
                                li.SubItems.Add(t)
                            Next
                            li.ForeColor = f.ItemForeColor
                            If Not f.SHA1ForeColor.Equals(Color.Black) Then
                                li.UseItemStyleForSubItems = False
                                li.SubItems(3).ForeColor = f.SHA1ForeColor
                            End If
                            If Not f.MD5ForeColor.Equals(Color.Black) Then
                                li.UseItemStyleForSubItems = False
                                li.SubItems(16).ForeColor = f.MD5ForeColor
                            End If
                            ListView1.Items.Add(li)
                        Next

                    End SyncLock
                    SyncLock d.contents.UnwrittenFiles
                        For Each f As ltfsindex.file In d.contents.UnwrittenFiles
                            Dim li As New ListViewItem
                            SyncLock f
                                li.Tag = f
                                li.Text = f.name
                                Dim s(15) As String
                                s(0) = f.length
                                s(1) = f.creationtime
                                s(2) = f.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True)
                                s(15) = f.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True)
                                s(3) = f.fileuid
                                s(4) = f.openforwrite
                                s(5) = f.readonly
                                s(6) = f.changetime
                                s(7) = f.modifytime
                                s(8) = f.accesstime
                                s(9) = f.backuptime
                                If f.tag IsNot Nothing Then
                                    s(10) = f.tag.ToString()
                                Else
                                    s(10) = ""
                                End If
                                If f.extentinfo IsNot Nothing Then
                                    If f.extentinfo.Count > 0 Then
                                        Try
                                            s(11) = (f.extentinfo(0).startblock.ToString())
                                            s(12) = (f.extentinfo(0).partition.ToString())
                                        Catch ex As Exception
                                            s(11) = ("-")
                                            s(12) = ("-")
                                        End Try
                                    End If
                                Else
                                    s(11) = ("-")
                                    s(12) = ("-")
                                End If
                                s(13) = IOManager.FormatSize(f.length)
                                If f.WrittenBytes > 0 Then
                                    s(14) = (IOManager.FormatSize(f.WrittenBytes))
                                Else
                                    s(14) = ("-")
                                End If
                                For Each t As String In s
                                    li.SubItems.Add(t)
                                Next
                            End SyncLock
                            li.ForeColor = Color.Gray
                            ListView1.Items.Add(li)
                        Next
                    End SyncLock
                    If IsSqliteTreeView Then
                        Dim dirTreeNodes As List(Of TreeNode)
                        If Not d.Loaded Then
                            Dim dirs = DirProvider.QueryDirListWithWhere($"ParentPath='{d.fullpath}' and  isdirectory=1", GetSqliteConnection(Barcode))
                            TreeView1.SelectedNode.Nodes.Clear()
                            For Each dd In dirs
                                Dim t As New TreeNode(dd.name)
                                t.Tag = dd
                                t.ImageIndex = 1
                                t.SelectedImageIndex = 1
                                t.StateImageIndex = 1
                                TreeView1.SelectedNode.Nodes.Add(t)
                            Next
                            d.Loaded = True
                        End If
                        For Each tt In TreeView1.SelectedNode.Nodes
                            Dim dd As ltfsindex.directory = tt.Tag
                            Dim childdirs As List(Of ltfsindex.directory)
                            If Not dd.Loaded Then
                                childdirs = DirProvider.QueryDirListWithWhere($"ParentPath='{dd.fullpath}' and  isdirectory=1", GetSqliteConnection(Barcode))
                                tt.Nodes.Clear()
                                For Each childdir In childdirs
                                    Dim ttt As New TreeNode(childdir.name)
                                    ttt.Tag = childdir
                                    ttt.ImageIndex = 1
                                    ttt.SelectedImageIndex = 1
                                    ttt.StateImageIndex = 1
                                    tt.Nodes.Add(ttt)
                                Next
                                dd.Loaded = True
                            End If
                        Next
                        'TreeView1.ExpandAll()
                    End If
                ElseIf TypeOf (TreeView1.SelectedNode.Tag) Is ltfsindex.file Then
                    Dim f As ltfsindex.file = TreeView1.SelectedNode.Tag
                    Dim t As String = f.GetXAttr("ltfscopygui.archive")
                    If t IsNot Nothing AndAlso t.ToLower = "true" Then
                        压缩索引ToolStripMenuItem.Visible = False
                        解压索引ToolStripMenuItem.Visible = True
                        提取ToolStripMenuItem1.Enabled = False
                        校验ToolStripMenuItem1.Enabled = False
                        重命名ToolStripMenuItem.Enabled = False
                        删除ToolStripMenuItem.Enabled = False
                        统计ToolStripMenuItem.Enabled = False
                    End If
                    ListView1.Items.Clear()
                End If

            Catch ex As Exception
                Console.WriteLine(ex.Message+ex.StackTrace)
                PrintMsg(My.Resources.ResText_NavErr)
            End Try
        End If
    End Sub
    Private Sub TreeView1_AfterSelect(sender As Object, e As TreeViewEventArgs) Handles TreeView1.AfterSelect
        TriggerTreeView1Event()
    End Sub
    Private Sub TreeView1_Click(sender As Object, e As EventArgs) Handles TreeView1.Click
        TriggerTreeView1Event()
    End Sub
    Private Sub TreeView1_NodeMouseClick(sender As Object, e As TreeNodeMouseClickEventArgs) Handles TreeView1.NodeMouseClick
        If e.Button = MouseButtons.Right Then
            TreeView1.SelectedNode = e.Node
        End If
    End Sub

    Public Function CheckUnindexedDataSizeLimit(Optional ByVal ForceFlush As Boolean = False) As Boolean
        If (IndexWriteInterval > 0 AndAlso TotalBytesUnindexed >= IndexWriteInterval) Or ForceFlush Then
            WriteCurrentIndex(False, False)
            TotalBytesUnindexed = 0
            Invoke(Sub() Text = GetLocInfo())
            Return True
        End If
        Return False
    End Function
    Public Sub WriteCurrentIndex(Optional ByVal GotoEOD As Boolean = True, Optional ByVal ClearCurrentStat As Boolean = True)
        PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
        If GotoEOD Then TapeUtils.Locate(TapeDrive, 0, DataPartition, TapeUtils.LocateDestType.EOD)
        Dim CurrentPos As TapeUtils.PositionData = GetPos
        PrintMsg($"Position = {CurrentPos.ToString()}", LogOnly:=True)
        If ExtraPartitionCount > 0 AndAlso schema IsNot Nothing AndAlso schema.location.partition <> CurrentPos.PartitionNumber Then
            Throw New Exception($"{My.Resources.ResText_CurPos}p{CurrentPos.PartitionNumber}b{CurrentPos.BlockNumber}{My.Resources.ResText_IndexNAllowed}")
            Exit Sub
        End If
        If ExtraPartitionCount > 0 AndAlso schema IsNot Nothing AndAlso schema.location.startblock >= CurrentPos.BlockNumber Then
            Throw New Exception($"{My.Resources.ResText_CurPos}p{CurrentPos.PartitionNumber}b{CurrentPos.BlockNumber}{My.Resources.ResText_IndexNAllowed}")
            Exit Sub
        End If
        TapeUtils.WriteFileMark(TapeDrive)
        schema.generationnumber += 1
        schema.updatetime = Now.ToUniversalTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")
        schema.location.partition = ltfsindex.PartitionLabel.b
        schema.previousgenerationlocation = New ltfsindex.PartitionDef With {.partition = schema.location.partition, .startblock = schema.location.startblock}
        CurrentPos = GetPos
        PrintMsg($"Position = {CurrentPos.ToString()}", LogOnly:=True)
        schema.location.startblock = CurrentPos.BlockNumber
        PrintMsg(My.Resources.ResText_GI)
        Dim tmpf As String = $"{Application.StartupPath}\LWI_{Now.ToString("yyyyMMdd_HHmmss.fffffff")}.tmp"
        schema.SaveFile(tmpf)
        'Dim sdata As Byte() = Encoding.UTF8.GetBytes(schema.GetSerializedText())
        PrintMsg(My.Resources.ResText_WI)
        'TapeUtils.Write(TapeDrive, sdata, plabel.blocksize)
        TapeUtils.Write(TapeDrive, tmpf, plabel.blocksize)
        IO.File.Delete(tmpf)
        'While sdata.Length > 0
        '    Dim wdata As Byte() = sdata.Take(Math.Min(plabel.blocksize, sdata.Length)).ToArray
        '    sdata = sdata.Skip(Math.Min(plabel.blocksize, sdata.Length)).ToArray()
        '    TapeUtils.Write(TapeDrive, wdata)
        '    If sdata.Length = 0 Then Exit While
        'End While
        TotalBytesUnindexed = 0
        If ClearCurrentStat Then
            CurrentBytesProcessed = 0
            CurrentFilesProcessed = 0
        End If
        TapeUtils.WriteFileMark(TapeDrive)
        PrintMsg(My.Resources.ResText_WIF)
        CurrentPos = GetPos
        CurrentHeight = CurrentPos.BlockNumber
        PrintMsg($"Position = {CurrentPos.ToString()}", LogOnly:=True)
        Modified = ExtraPartitionCount > 0
    End Sub
    Public Sub RefreshIndexPartition()
        Dim block1 As Long = schema.location.startblock
        If schema.location.partition = ltfsindex.PartitionLabel.a Then
            block1 = schema.previousgenerationlocation.startblock
        End If
        If ExtraPartitionCount > 0 Then
            PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
            PrintMsg(My.Resources.ResText_Locating)
            TapeUtils.Locate(TapeDrive, 3, IndexPartition, TapeUtils.LocateDestType.FileMark)
            Dim p As TapeUtils.PositionData = GetPos
            PrintMsg($"Position = {p.ToString()}", LogOnly:=True)
            TapeUtils.WriteFileMark(TapeDrive)
            PrintMsg($"Filemark Written", LogOnly:=True)
            If schema.location.partition = ltfsindex.PartitionLabel.b Then
                schema.previousgenerationlocation = New ltfsindex.PartitionDef With {.partition = schema.location.partition, .startblock = schema.location.startblock}
            End If
            p = GetPos
            PrintMsg($"Position = {p.ToString()}", LogOnly:=True)
            schema.location.startblock = p.BlockNumber
        End If
        'schema.previousgenerationlocation.partition = ltfsindex.PartitionLabel.b
        Dim block0 As Long = schema.location.startblock
        If ExtraPartitionCount > 0 Then
            schema.location.partition = ltfsindex.PartitionLabel.a
            PrintMsg(My.Resources.ResText_GI)
            Dim tmpf As String = $"{Application.StartupPath}\LWI_{Now.ToString("yyyyMMdd_HHmmss.fffffff")}.tmp"
            schema.SaveFile(tmpf)
            'Dim sdata As Byte() = Encoding.UTF8.GetBytes(schema.GetSerializedText())
            PrintMsg(My.Resources.ResText_WI)
            'TapeUtils.Write(TapeDrive, sdata, plabel.blocksize)
            TapeUtils.Write(TapeDrive, tmpf, plabel.blocksize)
            IO.File.Delete(tmpf)
            'While sdata.Length > 0
            '    Dim wdata As Byte() = sdata.Take(Math.Min(plabel.blocksize, sdata.Length)).ToArray
            '    sdata = sdata.Skip(Math.Min(plabel.blocksize, sdata.Length)).ToArray()
            '    TapeUtils.Write(TapeDrive, wdata)
            'End While
            TapeUtils.WriteFileMark(TapeDrive)
            PrintMsg(My.Resources.ResText_WIF)
            PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
        End If
        TapeUtils.WriteVCI(TapeDrive, schema.generationnumber, block0, block1, schema.volumeuuid.ToString(), ExtraPartitionCount)
        Modified = False
    End Sub
    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="Data"></param>
    ''' <param name="RetainPosisiton"></param>
    ''' <returns>
    ''' Data start position
    ''' </returns>
    Public Function DumpDataToIndexPartition(ByVal Data As IO.Stream, Optional ByVal RetainPosisiton As Boolean = True) As Long
        Try
            If ExtraPartitionCount = 0 Then Return -1
            'record previous position
            Dim pPrevious As New TapeUtils.PositionData(TapeDrive)
            'locate
            TapeUtils.Locate(TapeDrive, 3, IndexPartition, TapeUtils.LocateDestType.FileMark)
            Dim pFMIndex As New TapeUtils.PositionData(TapeDrive)
            Dim pStartBlock As Long = pFMIndex.BlockNumber
            'Dump old index
            If Not TapeUtils.ReadFileMark(TapeDrive) Then Return -1
            Dim tmpf As String = $"{Application.StartupPath}\LIT_{Now.ToString("yyyyMMdd_HHmmss.fffffff")}.tmp"
            TapeUtils.ReadToFileMark(TapeDrive, tmpf, plabel.blocksize)
            'Write data
            TapeUtils.Locate(TapeDrive, pFMIndex.BlockNumber, pFMIndex.PartitionNumber)
            TapeUtils.Write(TapeDrive, Data, plabel.blocksize)
            'Recover old index
            TapeUtils.WriteFileMark(TapeDrive)
            TapeUtils.Write(TapeDrive, tmpf, plabel.blocksize)
            IO.File.Delete(tmpf)
            TapeUtils.WriteFileMark(TapeDrive)
            'Recover position
            If RetainPosisiton Then TapeUtils.Locate(TapeDrive, pPrevious.BlockNumber, pPrevious.PartitionNumber)
            Return pStartBlock
        Catch ex As Exception
            MessageBox.Show(ex.ToString())
        End Try
        Return -1
    End Function
    Public Sub MoveToIndexPartition(ByVal f As ltfsindex.file)
        Try
            If ExtraPartitionCount = 0 Then Exit Sub
            If f Is Nothing Then Exit Sub
            If f.extentinfo Is Nothing OrElse f.extentinfo.Count = 0 Then Exit Sub
            If f.extentinfo(0).partition = ltfsindex.PartitionLabel.a Then Exit Sub
            Dim tmpf As String = $"{Application.StartupPath}\LFT_{Now.ToString("yyyyMMdd_HHmmss.fffffff")}.tmp"
            RestoreFile(tmpf, f)
            Dim fs As New IO.FileStream(tmpf, IO.FileMode.Open)
            Dim len As Long = fs.Length
            Dim startblock As Integer = DumpDataToIndexPartition(fs)
            f.extentinfo = {New ltfsindex.file.extent With {.startblock = startblock, .bytecount = len, .byteoffset = 0, .fileoffset = 0, .partition = ltfsindex.PartitionLabel.a}}.ToList()
            IO.File.Delete(tmpf)
        Catch ex As Exception
            MessageBox.Show(ex.ToString)
        End Try

    End Sub
    Public Function DumpDataToIndexPartition(ByVal Data As Byte(), Optional ByVal RetainPosisiton As Boolean = True) As Long
        Dim s As New IO.MemoryStream(Data)
        Return DumpDataToIndexPartition(s, RetainPosisiton)
    End Function
    Public Sub UpdataAllIndex()
        If (My.Settings.LTFSWriter_ForceIndex OrElse (TotalBytesUnindexed <> 0)) AndAlso schema IsNot Nothing AndAlso schema.location.partition = ltfsindex.PartitionLabel.b Then
            PrintMsg(My.Resources.ResText_UDI)
            WriteCurrentIndex(False)
        End If
        PrintMsg(My.Resources.ResText_UI)
        RefreshIndexPartition()
        AutoDump()
        TapeUtils.ReleaseUnit(TapeDrive)
        TapeUtils.AllowMediaRemoval(TapeDrive)
        PrintMsg(My.Resources.ResText_IUd)
        If schema IsNot Nothing AndAlso schema.location.partition = ltfsindex.PartitionLabel.a Then Me.Invoke(Sub() 更新数据区索引ToolStripMenuItem.Enabled = False)
        If SilentMode Then
            If SilentAutoEject Then
                TapeUtils.LoadEject(TapeDrive, TapeUtils.LoadOption.Eject)
                RaiseEvent TapeEjected()
            End If
        Else
            Dim DoEject As Boolean = False
            Invoke(Sub()
                       DoEject = WA3ToolStripMenuItem.Checked OrElse MessageBox.Show(My.Resources.ResText_PEj, My.Resources.ResText_Hint, MessageBoxButtons.OKCancel) = DialogResult.OK
                   End Sub)
            If DoEject Then
                TapeUtils.LoadEject(TapeDrive, TapeUtils.LoadOption.Eject)
                PrintMsg(My.Resources.ResText_Ejd)
                RaiseEvent TapeEjected()
            End If
        End If
        Invoke(Sub()
                   LockGUI(False)
                   RefreshDisplay()
               End Sub)
    End Sub
    Public Sub OnWriteFinished()
        If WA0ToolStripMenuItem.Checked Then Exit Sub
        If WA1ToolStripMenuItem.Checked Then
            Dim SilentBefore As Boolean = SilentMode
            SilentMode = True
            Try
                If (My.Settings.LTFSWriter_ForceIndex OrElse TotalBytesUnindexed <> 0) AndAlso schema IsNot Nothing AndAlso schema.location.partition = ltfsindex.PartitionLabel.b Then
                    WriteCurrentIndex(False)
                    TapeUtils.Flush(TapeDrive)
                End If
            Catch ex As Exception
                PrintMsg(ex.ToString())
            End Try
            SilentMode = SilentBefore
            Exit Sub
        End If
        If WA2ToolStripMenuItem.Checked Then
            Dim SilentBefore As Boolean = SilentMode
            SilentMode = True
            SilentAutoEject = False
            UpdataAllIndex()
            SilentMode = SilentBefore
            Exit Sub
        End If
        If WA3ToolStripMenuItem.Checked Then
            SilentMode = True
            SilentAutoEject = True
            UpdataAllIndex()
            Exit Sub
        End If
    End Sub
    Public Property ParallelAdd As Boolean = False
    Public ExplorerComparer As New ExplorerUtils()
    Public Function IsSameFile(f As IO.FileInfo, f0 As ltfsindex.file) As Boolean
        Dim Result As Boolean = True
        If f.Name <> f0.name Then
            Result = False
        ElseIf f.Length <> f0.length Then
            Result = False
        End If
        Dim validtime As String = ""
        Try
            validtime = f.LastWriteTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")
            If validtime <> f0.modifytime Then
                Result = False
            End If
        Catch ex As Exception
            validtime = f.CreationTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")
            If validtime <> f0.modifytime Then
                Result = False
            End If
        End Try
        If Not Result Then
            PrintMsg($"Different File: {f0.name}|{f0.length}|{f0.modifytime}->{f.Name}|{f.Length}|{validtime}", LogOnly:=True, Warning:=True)
        End If
        Return Result
    End Function
    Public Sub AddFile(f As IO.FileInfo, d As ltfsindex.directory, Optional ByVal OverWrite As Boolean = False)
        Try
            If StopFlag Then Exit Sub
            If f.Extension.ToLower = ".xattr" Then Exit Sub
            'symlink
            If My.Settings.LTFSWriter_SkipSymlink AndAlso ((f.Attributes And IO.FileAttributes.ReparsePoint) <> 0) Then
                Exit Sub
            End If
            Dim FileExist As Boolean = False
            Dim SameFile As Boolean = False
            '检查磁带已有文件
            SyncLock d.contents._file
                For i As Integer = d.contents._file.Count - 1 To 0 Step -1
                    Dim oldf As ltfsindex.file = d.contents._file(i)
                    If oldf.name.ToLower = f.Name.ToLower Then
                        SameFile = IsSameFile(f, oldf)
                        If OverWrite And Not SameFile Then d.contents._file.RemoveAt(i)
                        FileExist = True
                    End If
                Next
            End SyncLock
            If FileExist And (SameFile OrElse Not OverWrite) Then Exit Sub
            '检查写入队列
            If Not FileExist Then
                While True
                    Threading.Thread.Sleep(0)
                    SyncLock UFReadCount
                        If UFReadCount > 0 Then Continue While

                        For i As Integer = d.contents.LastUnwrittenFilesCount - 1 To 0 Step -1
                            If f.Name.ToLower = d.contents.UnwrittenFiles(i).name.ToLower Then
                                d.contents.UnwrittenFiles.RemoveAt(i)
                                For j As Integer = UnwrittenFiles.Count - 1 To 0 Step -1
                                    Dim oldf As FileRecord = UnwrittenFiles(j)
                                    If oldf.ParentDirectory Is d AndAlso oldf.File.name.ToLower = f.Name.ToLower Then
                                        UnwrittenFiles.RemoveAt(j)
                                        FileExist = True
                                        Exit For
                                    End If
                                Next
                                Exit For
                            End If
                        Next
                        Exit While
                    End SyncLock
                End While
            End If
            '添加到队列
            Dim frnew As New FileRecord(f.FullName, d)
            While True
                Threading.Thread.Sleep(0)
                SyncLock UFReadCount
                    If UFReadCount > 0 Then Continue While
                    UnwrittenFiles.Add(frnew)
                    Exit While
                End SyncLock
            End While
        Catch ex As Exception
            Invoke(Sub() MessageBox.Show(ex.ToString()))
        End Try
    End Sub
    Public Sub AddDirectry(dnew1 As IO.DirectoryInfo, d1 As ltfsindex.directory, Optional ByVal OverWrite As Boolean = False)
        If My.Settings.LTFSWriter_SkipSymlink AndAlso ((dnew1.Attributes And IO.FileAttributes.ReparsePoint) <> 0) Then
            Exit Sub
        End If
        If StopFlag Then Exit Sub
        Dim dirExist As Boolean = False
        Dim dT As ltfsindex.directory = Nothing
        SyncLock d1.contents._directory
            For Each fe As ltfsindex.directory In d1.contents._directory
                If fe.name = dnew1.Name Then
                    dirExist = True
                    dT = fe
                    Exit For
                End If
            Next
        End SyncLock
        If Not dirExist Then
            dT = New ltfsindex.directory With {
                  .name = dnew1.Name,
                  .creationtime = dnew1.CreationTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z"),
                  .fileuid = schema.highestfileuid + 1,
                  .accesstime = dnew1.LastAccessTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z"),
                  .modifytime = dnew1.LastWriteTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z"),
                  .changetime = .modifytime,
                  .backuptime = Now.ToUniversalTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z"),
                  .readonly = False
                  }
            d1.contents._directory.Add(dT)
            Threading.Interlocked.Increment(schema.highestfileuid)
            DirProvider.UpdateHightestFileUid(GetSqliteConnection(Barcode), Barcode, schema.highestfileuid)
        End If
        If Not ParallelAdd Then
            'PrintMsg($"添加目录: {dnew1.FullName} ")
            Dim startTimestamp = DateTime.Now
            Dim flist As List(Of IO.FileInfo) = dnew1.GetFiles().ToList()
            Metric.FileOperationDurationHistogram.WithLabels(Barcode, "dnew1.GetFiles().ToList()", False).Observe((DateTime.Now - startTimestamp).TotalMilliseconds)
            'PrintMsg($"添加目录: {dnew1.FullName} 文件数: {flist.Count}")
            flist.Sort(New Comparison(Of IO.FileInfo)(Function(a As IO.FileInfo, b As IO.FileInfo) As Integer
                                                          Return ExplorerComparer.Compare(a.Name, b.Name)
                                                      End Function))
            Dim dictionary = dT.contents._file.ToDictionary(Function(n) n.name, Function(n) n)
            For Each f As IO.FileInfo In flist
                Try
                    Metric.OperationProcessedGauge.WithLabels(Barcode, "add_file").Inc()
                    Dim FileExist As Boolean = False
                    Dim SameFile As Boolean = False
                    If f.Extension.ToLower = ".xattr" Then Continue For
                    '检查已有文件
                    SyncLock dT.contents._file
                        If dictionary.ContainsKey(f.Name) Then
                            FileExist = True
                            Dim fe = dictionary(f.Name)
                            SameFile = IsSameFile(f, fe)
                            If OverWrite And Not SameFile Then
                                Dim startTimestamp2 = DateTime.Now
                                dT.contents._file.Remove(fe)
                                Metric.FileOperationDurationHistogram.WithLabels(Barcode, "dT.contents._file.Remove(fe)", "").Observe((DateTime.Now - startTimestamp2).TotalMilliseconds)
                                Metric.OperationProcessedGauge.WithLabels(Barcode, "add_file OverWrite And Not SameFile").Inc()
                            End If
                        End If
                    End SyncLock
                    If FileExist And (SameFile OrElse Not OverWrite) Then Continue For
                    '检查写入队列
                    If Not FileExist Then
                        While True
                            Threading.Thread.Sleep(0)
                            SyncLock UFReadCount
                                If UFReadCount > 0 Then Continue While
                                For i As Integer = dT.contents.LastUnwrittenFilesCount - 1 To 0 Step -1
                                    If dT.contents.UnwrittenFiles(i).name.ToLower = f.Name.ToLower Then
                                        dT.contents.UnwrittenFiles.RemoveAt(i)
                                        For j As Integer = UnwrittenFiles.Count - 1 To 0 Step -1
                                            Dim oldf As FileRecord = UnwrittenFiles(j)
                                            If oldf.ParentDirectory Is dT AndAlso oldf.File.name.ToLower = f.Name.ToLower Then
                                                UnwrittenFiles.RemoveAt(j)
                                                FileExist = True
                                                Exit For
                                            End If
                                        Next
                                        Exit For
                                    End If
                                Next
                                Exit While
                            End SyncLock
                        End While
                    End If
                    '添加到队列
                    While True
                        Threading.Thread.Sleep(0)
                        SyncLock UFReadCount
                            If UFReadCount > 0 Then Continue While
                            UnwrittenFiles.Add(New FileRecord(f.FullName, dT))
                            Exit While
                        End SyncLock
                    End While
                Catch ex As Exception
                    Invoke(Sub() MessageBox.Show(ex.ToString()))
                End Try
            Next
        Else
            Parallel.ForEach(dnew1.GetFiles(),
                Sub(f As IO.FileInfo)
                    Try
                        Dim FileExist As Boolean = False
                        Dim SameFile As Boolean = False
                        If f.Extension.ToLower = ".xattr" Then Exit Sub
                        '检查已有文件
                        SyncLock dT.contents._file
                            For i As Integer = dT.contents._file.Count - 1 To 0 Step -1
                                Dim fe As ltfsindex.file = dT.contents._file(i)
                                If fe.name = f.Name Then
                                    FileExist = True
                                    SameFile = IsSameFile(f, fe)
                                    If OverWrite And Not SameFile Then dT.contents._file.RemoveAt(i)
                                End If
                            Next
                        End SyncLock
                        If FileExist And (SameFile OrElse Not OverWrite) Then Exit Sub
                        '检查写入队列
                        If Not FileExist Then
                            While True
                                Threading.Thread.Sleep(0)
                                SyncLock UFReadCount
                                    If UFReadCount > 0 Then Continue While
                                    For i As Integer = UnwrittenFiles.Count - 1 To 0 Step -1
                                        Dim oldf As FileRecord = UnwrittenFiles(i)
                                        If oldf.ParentDirectory Is dT AndAlso oldf.File.name.ToLower = f.Name.ToLower Then
                                            oldf.ParentDirectory.contents.UnwrittenFiles.Remove(oldf.File)
                                            UnwrittenFiles.RemoveAt(i)
                                            FileExist = True
                                            Exit For
                                        End If
                                    Next
                                    Exit While
                                End SyncLock
                            End While
                        End If
                        '添加到队列
                        While True
                            Threading.Thread.Sleep(0)
                            SyncLock UFReadCount
                                If UFReadCount > 0 Then Continue While
                                UnwrittenFiles.Add(New FileRecord(f.FullName, dT))
                                Exit While
                            End SyncLock
                        End While
                    Catch ex As Exception
                        Invoke(Sub() MessageBox.Show(ex.ToString()))
                    End Try
                End Sub)
        End If
        Dim dl As List(Of IO.DirectoryInfo) = dnew1.GetDirectories().ToList()
        dl.Sort(New Comparison(Of IO.DirectoryInfo)(Function(a As IO.DirectoryInfo, b As IO.DirectoryInfo) As Integer
                                                        Return ExplorerComparer.Compare(a.Name, b.Name)
                                                    End Function))
        For Each dn As IO.DirectoryInfo In dnew1.GetDirectories()
            AddDirectry(dn, dT, OverWrite)
        Next
    End Sub
    Public Sub ConcatDirectory(dnew1 As IO.DirectoryInfo, d1 As ltfsindex.directory, Optional ByVal OverWrite As Boolean = False)
        For Each f As IO.FileInfo In dnew1.GetFiles()
            Dim FileExist As Boolean = False
            '检查磁带已有文件
            SyncLock d1.contents._file
                For Each fe As ltfsindex.file In d1.contents._file
                    If fe.name = f.Name Then
                        FileExist = True
                        If OverWrite Then
                            d1.contents._file.Remove(fe)
                        End If
                    End If
                Next
            End SyncLock
            If (Not OverWrite) And FileExist Then Continue For
            '检查写入队列
            If Not FileExist Then
                While True
                    Threading.Thread.Sleep(0)
                    SyncLock UFReadCount
                        If UFReadCount > 0 Then Continue While
                        For Each oldf As FileRecord In UnwrittenFiles
                            If oldf.ParentDirectory Is d1 AndAlso oldf.File.name.ToLower = f.Name.ToLower Then
                                oldf.ParentDirectory.contents.UnwrittenFiles.Remove(oldf.File)
                                UnwrittenFiles.Remove(oldf)
                                FileExist = True
                                Exit For
                            End If
                        Next
                        Exit While
                    End SyncLock
                End While
            End If
            '添加到队列
            While True
                Threading.Thread.Sleep(0)
                SyncLock UFReadCount
                    If UFReadCount > 0 Then Continue While
                    UnwrittenFiles.Add(New FileRecord(f.FullName, d1))
                    Exit While
                End SyncLock
            End While
        Next
        For Each dn As IO.DirectoryInfo In dnew1.GetDirectories()
            Dim dirExist As Boolean = False
            Dim dT As ltfsindex.directory = Nothing
            SyncLock d1.contents._directory
                For Each fe As ltfsindex.directory In d1.contents._directory
                    If fe.name = dn.Name Then
                        dirExist = True
                        dT = fe
                        Exit For
                    End If
                Next
            End SyncLock

            If Not dirExist Then
                dT = New ltfsindex.directory With {
                                            .name = dn.Name,
                                            .creationtime = dn.CreationTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z"),
                                            .fileuid = schema.highestfileuid + 1,
                                            .accesstime = dn.LastAccessTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z"),
                                            .modifytime = dn.LastWriteTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z"),
                                            .changetime = .modifytime,
                                            .backuptime = Now.ToUniversalTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z"),
                                            .readonly = False
                                            }
                d1.contents._directory.Add(dT)
                schema.highestfileuid += 1
            End If
            ConcatDirectory(dn, dT, OverWrite)
        Next
    End Sub
    Public Sub DeleteDir()
        If TreeView1.SelectedNode IsNot Nothing Then
            Dim d As ltfsindex.directory = TreeView1.SelectedNode.Tag
            If TreeView1.SelectedNode.Parent IsNot Nothing AndAlso MessageBox.Show($"{My.Resources.ResText_DelConfrm}{d.name}", My.Resources.ResText_Confirm, MessageBoxButtons.OKCancel) = DialogResult.OK Then
                If IsSqliteTreeView Then
                    Try
                        DirProvider.DeleteDir(d.fullpath, GetSqliteConnection(Barcode), Barcode)
                    Catch ex As Exception
                        Console.WriteLine(ex.Message + ex.StackTrace)
                        MessageBox.Show(ex.Message)

                    End Try

                Else
                    Dim pd As ltfsindex.directory = TreeView1.SelectedNode.Parent.Tag
                    pd.contents._directory.Remove(d)
                    If TotalBytesUnindexed = 0 Then TotalBytesUnindexed = 1
                    Dim IterAllDirectory As Action(Of ltfsindex.directory) =
                        Sub(d1 As ltfsindex.directory)
                            Dim RList As New List(Of FileRecord)
                            SyncLock d1.contents.UnwrittenFiles
                                For Each f As ltfsindex.file In d1.contents.UnwrittenFiles
                                    UFReadCount.Inc()
                                    For Each fr As FileRecord In UnwrittenFiles
                                        If fr.File Is f Then
                                            RList.Add(fr)
                                        End If
                                    Next
                                    UFReadCount.Dec()
                                Next
                            End SyncLock

                            For Each fr As FileRecord In RList
                                While True
                                    Threading.Thread.Sleep(0)
                                    SyncLock UFReadCount
                                        If UFReadCount > 0 Then Continue While
                                        UnwrittenFiles.Remove(fr)
                                        Exit While
                                    End SyncLock
                                End While
                            Next
                            SyncLock d1.contents._directory
                                For Each d2 As ltfsindex.directory In d1.contents._directory
                                    IterAllDirectory(d2)
                                Next
                            End SyncLock

                        End Sub
                    IterAllDirectory(d)
                    If TreeView1.SelectedNode.Parent IsNot Nothing AndAlso TreeView1.SelectedNode.Parent.Tag IsNot Nothing AndAlso TypeOf (TreeView1.SelectedNode.Parent.Tag) Is ltfsindex.directory Then
                        TreeView1.SelectedNode = TreeView1.SelectedNode.Parent
                    End If
                    If TotalBytesUnindexed = 0 Then TotalBytesUnindexed = 1
                End If

                RefreshDisplay()
            End If
        End If

    End Sub
    Public Sub RenameDir()
        If TreeView1.SelectedNode IsNot Nothing Then
            Dim d As ltfsindex.directory = TreeView1.SelectedNode.Tag
            Dim s As String = InputBox(My.Resources.ResText_DirName, My.Resources.ResText_RenameDir, d.name)
            If s <> "" Then
                If s = d.name Then Exit Sub
                If (s.IndexOfAny(System.IO.Path.GetInvalidFileNameChars()) >= 0) Then
                    MessageBox.Show(My.Resources.ResText_DirNIllegal)
                    Exit Sub
                End If
                If TreeView1.SelectedNode.Parent IsNot Nothing Then
                    Dim pd As ltfsindex.directory = TreeView1.SelectedNode.Parent.Tag
                    SyncLock pd.contents._directory
                        For Each d2 As ltfsindex.directory In pd.contents._directory
                            If d2 IsNot d And d2.name = s Then
                                MessageBox.Show(My.Resources.ResText_DirNExist)
                                Exit Sub
                            End If
                        Next
                    End SyncLock
                End If
                d.name = s
                If TotalBytesUnindexed = 0 Then TotalBytesUnindexed = 1
                RefreshDisplay()
            End If
        End If

    End Sub
    Private Sub 重命名ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 重命名文件ToolStripMenuItem.Click
        If ListView1.Tag IsNot Nothing AndAlso
        ListView1.SelectedItems IsNot Nothing AndAlso
        ListView1.SelectedItems.Count > 0 AndAlso
        ListView1.SelectedItems.Item(0).Tag IsNot Nothing AndAlso
        TypeOf (ListView1.SelectedItems.Item(0).Tag) Is ltfsindex.file Then
            Dim f As ltfsindex.file = ListView1.SelectedItems.Item(0).Tag
            Dim d As ltfsindex.directory = ListView1.Tag
            Dim newname As String = InputBox(My.Resources.ResText_NFName, My.Resources.ResText_Rename, f.name)
            If newname = f.name Then Exit Sub
            If newname = "" Then Exit Sub
            If (newname.IndexOfAny(System.IO.Path.GetInvalidFileNameChars()) >= 0) Then
                MessageBox.Show(My.Resources.ResText_FNIllegal)
                Exit Sub
            End If
            SyncLock d.contents._file
                For Each allf As ltfsindex.file In d.contents._file
                    If allf IsNot f And allf.name.ToLower = newname.ToLower Then
                        MessageBox.Show(My.Resources.ResText_FNExist)
                        Exit Sub
                    End If
                Next
            End SyncLock
            f.name = newname
            If TotalBytesUnindexed = 0 Then TotalBytesUnindexed = 1
            RefreshDisplay()
        End If
    End Sub
    Private Sub 删除文件ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 删除文件ToolStripMenuItem.Click
        If ListView1.Tag IsNot Nothing AndAlso
        ListView1.SelectedItems IsNot Nothing AndAlso
        ListView1.SelectedItems.Count > 0 AndAlso
        MessageBox.Show($"{My.Resources.ResText_DelConfrm}{ListView1.SelectedItems.Count}{My.Resources.ResText_Files_C}", My.Resources.ResText_Warning, MessageBoxButtons.OKCancel) = DialogResult.OK Then
            SyncLock ListView1.SelectedItems
                For Each ItemSelected As ListViewItem In ListView1.SelectedItems
                    If ItemSelected.Tag IsNot Nothing AndAlso TypeOf (ItemSelected.Tag) Is ltfsindex.file Then
                        Dim f As ltfsindex.file = ItemSelected.Tag
                        Dim d As ltfsindex.directory = ListView1.Tag
                        If IsSqliteTreeView Then
                            DirProvider.DeleteFile(f, GetSqliteConnection(Barcode), Barcode)
                        End If
                        If d.contents.UnwrittenFiles.Contains(f) Then
                            While True
                                Threading.Thread.Sleep(0)
                                SyncLock UFReadCount
                                    If UFReadCount > 0 Then Continue While
                                    For Each fr As FileRecord In UnwrittenFiles
                                        If fr.File Is f Then
                                            fr.RemoveUnwritten()
                                            UnwrittenFiles.Remove(fr)
                                            Exit For
                                        End If
                                    Next
                                    Exit While
                                End SyncLock
                            End While
                        End If
                        If d.contents._file.Contains(f) Then
                            d.contents._file.Remove(f)
                            If TotalBytesUnindexed = 0 Then TotalBytesUnindexed = 1
                        End If
                    End If
                Next
            End SyncLock
            RefreshDisplay()
        End If
    End Sub
    Private Sub 添加文件ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 添加文件ToolStripMenuItem.Click
        If ListView1.Tag IsNot Nothing AndAlso OpenFileDialog1.ShowDialog = DialogResult.OK Then
            Dim d As ltfsindex.directory = ListView1.Tag
            Dim overwrite As Boolean = 覆盖已有文件ToolStripMenuItem.Checked
            AddFileOrDir(d, OpenFileDialog1.FileNames, overwrite)
            'For Each fpath As String In OpenFileDialog1.FileNames
            '    Dim f As IO.FileInfo = New IO.FileInfo(fpath)
            '    Try
            '        AddFile(f, d, 覆盖已有文件ToolStripMenuItem.Checked)
            '        PrintMsg("文件添加成功")
            '    Catch ex As Exception
            '        PrintMsg("文件添加失败")
            '        MessageBox.Show(ex.ToString())
            '    End Try
            'Next
            'RefreshDisplay()
        End If
    End Sub
    Public Sub AddFileOrDir(d As ltfsindex.directory, Paths As String(), Optional ByVal overwrite As Boolean = False)
        Dim th As New Threading.Thread(
                Sub()
                    StopFlag = False
                    PrintMsg($"{My.Resources.ResText_Adding}{Paths.Length}{My.Resources.ResText_Items_x}")
                    Dim numi As Integer = 0
                    Dim PList As List(Of String) = Paths.ToList()
                    PList.Sort(ExplorerComparer)
                    ltfsindex.WSort({d}.ToList, Nothing, Sub(d1 As ltfsindex.directory)
                                                             d1.contents.LastUnwrittenFilesCount = d1.contents.UnwrittenFiles.Count
                                                         End Sub)
                    For Each path As String In PList
                        If Not path.StartsWith("\\") Then path = $"\\?\{path}"
                        Dim i As Integer = Threading.Interlocked.Increment(numi)
                        If StopFlag Then Exit For
                        Try
                            If IO.File.Exists(path) Then
                                Dim f As IO.FileInfo = New IO.FileInfo(path)
                                PrintMsg($"{My.Resources.ResText_Adding} [{i}/{Paths.Length}] {f.Name}")
                                AddFile(f, d, overwrite)
                            ElseIf IO.Directory.Exists(path) Then
                                Dim f As IO.DirectoryInfo = New IO.DirectoryInfo(path)
                                PrintMsg($"{My.Resources.ResText_Adding} [{i}/{Paths.Length}] {f.Name}")
                                AddDirectry(f, d, overwrite)
                            End If
                        Catch ex As Exception
                            Invoke(Sub() MessageBox.Show(ex.ToString()))
                        End Try
                    Next

                    If ParallelAdd Then UnwrittenFiles.Sort(New Comparison(Of FileRecord)(Function(a As FileRecord, b As FileRecord) As Integer
                                                                                              Return ExplorerComparer.Compare(a.SourcePath, b.SourcePath)
                                                                                          End Function))
                    StopFlag = False
                    RefreshDisplay()
                    PrintMsg(My.Resources.ResText_AddFin)
                    LockGUI(False)
                End Sub)
        LockGUI()
        th.Start()
    End Sub
    Private Sub ListView1_DragEnter(sender As Object, e As DragEventArgs) Handles ListView1.DragEnter
        If Not AllowOperation OrElse Not MenuStrip1.Enabled Then
            PrintMsg(My.Resources.ResText_DragNA)
            Exit Sub
        End If
        If ListView1.Tag IsNot Nothing AndAlso TypeOf ListView1.Tag Is ltfsindex.directory Then
            Dim Paths As String() = e.Data.GetData(GetType(String()))
            Dim d As ltfsindex.directory = ListView1.Tag
            Dim overwrite As Boolean = 覆盖已有文件ToolStripMenuItem.Checked
            AddFileOrDir(d, Paths, overwrite)
        End If
    End Sub
    Private Sub 导入文件ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 导入文件ToolStripMenuItem.Click
        If ListView1.Tag IsNot Nothing AndAlso FolderBrowserDialog1.ShowDialog = DialogResult.OK Then
            Dim dnew As IO.DirectoryInfo = New IO.DirectoryInfo(FolderBrowserDialog1.SelectedPath)
            Dim Paths As New List(Of String)
            For Each f As IO.FileInfo In dnew.GetFiles("*", IO.SearchOption.TopDirectoryOnly)
                Paths.Add(f.FullName)
            Next
            For Each f As IO.DirectoryInfo In dnew.GetDirectories("*", IO.SearchOption.TopDirectoryOnly)
                Paths.Add(f.FullName)
            Next
            Dim d As ltfsindex.directory = ListView1.Tag
            AddFileOrDir(d, Paths.ToArray(), 覆盖已有文件ToolStripMenuItem.Checked)
            'Try
            '    ConcatDirectory(dnew, d, 覆盖已有文件ToolStripMenuItem.Checked)
            '    PrintMsg("导入成功")
            'Catch ex As Exception
            '    PrintMsg("导入失败")
            '    MessageBox.Show(ex.ToString())
            'End Try
            'RefreshDisplay()
        End If
    End Sub
    Private Sub 添加目录ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 添加目录ToolStripMenuItem.Click
        If ListView1.Tag IsNot Nothing Then
            Dim COFD As New CommonOpenFileDialog
            COFD.Multiselect = True
            COFD.IsFolderPicker = True
            If COFD.ShowDialog = CommonFileDialogResult.Ok Then
                Dim d As ltfsindex.directory = ListView1.Tag
                Dim dirs As New List(Of String)
                For Each fn As String In COFD.FileNames
                    dirs.Add(fn)
                Next
                AddFileOrDir(ListView1.Tag, dirs.ToArray(), 覆盖已有文件ToolStripMenuItem.Checked)
                'For Each dirSelected As String In COFD.FileNames
                '    Dim dnew As IO.DirectoryInfo = New IO.DirectoryInfo(dirSelected)
                '    Try
                '        AddDirectry(dnew, d, 覆盖已有文件ToolStripMenuItem.Checked)
                '        PrintMsg("目录添加成功")
                '    Catch ex As Exception
                '        PrintMsg("目录添加失败")
                '        MessageBox.Show(ex.ToString())
                '    End Try
                'Next
                'RefreshDisplay()
            End If
        End If
    End Sub
    Private Sub 新建目录ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 新建目录ToolStripMenuItem.Click
        If ListView1.Tag IsNot Nothing Then
            Dim s As String = InputBox(My.Resources.ResText_DirName, My.Resources.ResText_NewDir, "")
            If s <> "" Then
                If (s.IndexOfAny(System.IO.Path.GetInvalidFileNameChars()) >= 0) Then
                    MessageBox.Show(My.Resources.ResText_DirNIllegal)
                    Exit Sub
                End If
                Dim d As ltfsindex.directory = ListView1.Tag
                SyncLock d.contents._directory
                    For Each dold As ltfsindex.directory In d.contents._directory
                        If dold IsNot d And dold.name = s Then
                            MessageBox.Show(My.Resources.ResText_DirNExist)
                            Exit Sub
                        End If
                    Next
                End SyncLock

                Dim newdir As New ltfsindex.directory With {
                    .name = s,
                    .creationtime = Now.ToUniversalTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z"),
                    .fileuid = schema.highestfileuid + 1,
                    .backuptime = .creationtime,
                    .accesstime = .creationtime,
                    .changetime = .creationtime,
                    .modifytime = .creationtime,
                    .readonly = False
                    }
                schema.highestfileuid += 1
                d.contents._directory.Add(newdir)
                RefreshDisplay()
            End If
        End If
    End Sub
    Private Sub 删除目录ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 删除目录ToolStripMenuItem.Click
        DeleteDir()
    End Sub
    Private Sub 重命名目录ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 重命名目录ToolStripMenuItem.Click
        RenameDir()
    End Sub
    '每次都额外操作时候必须清空，locate到EOD，为了让winfsp挂载时候拷贝不会把前面的数据给覆盖掉
    Public Shared WinFspPositionData As TapeUtils.PositionData
    Public RestorePosition As TapeUtils.PositionData
    Public Sub RestoreFile(FileName As String, FileIndex As ltfsindex.file)
        Try
            If Not FileName.StartsWith("\\") Then FileName = $"\\?\{FileName}"
            Dim FileExist As Boolean = True
            If Not IO.File.Exists(FileName) Then
                FileExist = False
            Else
                Dim finfo As New IO.FileInfo(FileName)
                If finfo.Length <> FileIndex.length Then
                    FileExist = False
                ElseIf finfo.CreationTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z") <> FileIndex.creationtime Then
                    FileExist = False
                    'ElseIf finfo.LastAccessTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z") <> FileIndex.accesstime Then
                    '    FileExist = False
                ElseIf finfo.LastWriteTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z") <> FileIndex.modifytime Then
                    FileExist = False
                End If
                If Not FileExist Then PrintMsg($"{My.Resources.ResText_OverwritingDF}{FileName} {finfo.Length}->{FileIndex.length}{vbCrLf _
                                                  }ct{finfo.CreationTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")}->{FileIndex.creationtime}{vbCrLf _
                                                  }at{finfo.LastAccessTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")}->{FileIndex.accesstime}{vbCrLf _
                                                  }wt{finfo.LastWriteTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")}->{FileIndex.modifytime}{vbCrLf _
                                                  }", LogOnly:=True)
            End If
            If FileExist Then
                Threading.Interlocked.Increment(CurrentFilesProcessed)
                Threading.Interlocked.Increment(TotalFilesProcessed)

                Exit Sub
            End If
            If IO.File.Exists(FileName) Then
                Dim fi As New IO.FileInfo(FileName)
                fi.Attributes = fi.Attributes And Not IO.FileAttributes.ReadOnly
                IO.File.Delete(FileName)
            End If
            IO.File.WriteAllBytes(FileName, {})
            If FileIndex.length > 0 Then
                Dim reffile As String = ""
                If FileIndex.TempObj IsNot Nothing AndAlso TypeOf FileIndex.TempObj Is ltfsindex.file.refFile Then reffile = CType(FileIndex.TempObj, ltfsindex.file.refFile).FileName.ToString()
                If reffile <> "" AndAlso IO.File.Exists(reffile) Then
                    IO.File.Copy(reffile, FileName, True)
                    Dim finfo As New IO.FileInfo(FileName)
                    finfo.CreationTimeUtc = TapeUtils.ParseTimeStamp(FileIndex.creationtime)
                    finfo.LastWriteTimeUtc = TapeUtils.ParseTimeStamp(FileIndex.modifytime)
                    finfo.IsReadOnly = FileIndex.readonly
                    finfo.LastAccessTimeUtc = TapeUtils.ParseTimeStamp(FileIndex.accesstime)
                    Threading.Interlocked.Add(TotalBytesProcessed, FileIndex.length)
                    Threading.Interlocked.Add(CurrentBytesProcessed, FileIndex.length)
                Else
                    If FileIndex.TempObj Is Nothing OrElse TypeOf FileIndex.TempObj IsNot ltfsindex.file.refFile Then FileIndex.TempObj = New ltfsindex.file.refFile()
                    CType(FileIndex.TempObj, ltfsindex.file.refFile).FileName = FileName
                    Dim fs As New IO.FileStream(FileName, IO.FileMode.OpenOrCreate, IO.FileAccess.ReadWrite, IO.FileShare.Read, 8388608, IO.FileOptions.None)
                    Try
                        FileIndex.extentinfo.Sort(New Comparison(Of ltfsindex.file.extent)(Function(a As ltfsindex.file.extent, b As ltfsindex.file.extent)
                                                                                               If a.startblock <> b.startblock Then Return a.startblock.CompareTo(b.startblock)
                                                                                               Return a.fileoffset.CompareTo(b.fileoffset)
                                                                                           End Function))
                        For Each fe As ltfsindex.file.extent In FileIndex.extentinfo
                            Dim succ As Boolean = False
                            Do
                                Dim BlockAddress As Long = fe.startblock
                                Dim ByteOffset As Long = fe.byteoffset
                                Dim FileOffset As Long = fe.fileoffset
                                Dim Partition As Long = fe.partition
                                Dim TotalBytes As Long = fe.bytecount
                                'Dim p As New TapeUtils.PositionData(TapeDrive)
                                If RestorePosition Is Nothing OrElse RestorePosition.BlockNumber <> BlockAddress OrElse RestorePosition.PartitionNumber <> GetPartitionNumber(fe.partition) Then
                                    TapeUtils.Locate(TapeDrive, BlockAddress, GetPartitionNumber(Partition), TapeUtils.LocateDestType.Block)
                                    RestorePosition = New TapeUtils.PositionData(TapeDrive)
                                End If
                                fs.Seek(FileOffset, IO.SeekOrigin.Begin)
                                Dim ReadedSize As Long = 0
                                While (ReadedSize < TotalBytes + ByteOffset) And Not StopFlag
                                    Dim CurrentBlockLen As Integer = Math.Min(plabel.blocksize, TotalBytes + ByteOffset - ReadedSize)
                                    Dim Data As Byte() = TapeUtils.ReadBlock(TapeDrive, Nothing, CurrentBlockLen, True)
                                    SyncLock RestorePosition
                                        RestorePosition.BlockNumber += 1
                                    End SyncLock
                                    CurrentBlockLen = Math.Min(Data.Length, TotalBytes + ByteOffset - ReadedSize)
                                    '                                If Data.Length <> CurrentBlockLen OrElse CurrentBlockLen = 0 Then
                                    '                                    PrintMsg($"Error reading at p{RestorePosition.PartitionNumber}b{RestorePosition.BlockNumber}: readed length {Data.Length} should be {CurrentBlockLen}", LogOnly:=True, ForceLog:=True)
                                    '                                    succ = False
                                    '                                    Exit Do
                                    '                                End If
                                    ReadedSize += CurrentBlockLen - ByteOffset
                                    fs.Write(Data, ByteOffset, CurrentBlockLen - ByteOffset)
                                    Threading.Interlocked.Add(TotalBytesProcessed, CurrentBlockLen - ByteOffset)
                                    Threading.Interlocked.Add(CurrentBytesProcessed, CurrentBlockLen - ByteOffset)
                                    ByteOffset = 0
                                    While Pause
                                        Threading.Thread.Sleep(10)
                                    End While
                                End While
                                If StopFlag Then
                                    fs.Close()
                                    IO.File.Delete(FileName)
                                    succ = True
                                    Exit Do
                                End If
                                succ = True
                                Exit Do
                            Loop

                            If Not succ Then
                                PrintMsg($"{FileIndex.name}{My.Resources.ResText_RestoreErr}", ForceLog:=True)
                                Exit For
                            End If
                            If StopFlag Then Exit Sub
                        Next
                    Catch ex As Exception
                        PrintMsg($"{FileIndex.name}{My.Resources.ResText_RestoreErr}{ex.ToString}", ForceLog:=True)
                    End Try

                    fs.Flush()
                    fs.Close()
                    Dim finfo As New IO.FileInfo(FileName)
                    Try
                        finfo.CreationTimeUtc = TapeUtils.ParseTimeStamp(FileIndex.creationtime)
                        finfo.LastWriteTimeUtc = TapeUtils.ParseTimeStamp(FileIndex.modifytime)
                        finfo.IsReadOnly = FileIndex.readonly
                        finfo.LastAccessTimeUtc = TapeUtils.ParseTimeStamp(FileIndex.accesstime)

                    Catch ex As Exception

                    End Try
                End If

            Else
                Dim finfo As New IO.FileInfo(FileName)
                finfo.CreationTimeUtc = TapeUtils.ParseTimeStamp(FileIndex.creationtime)
                finfo.LastWriteTimeUtc = TapeUtils.ParseTimeStamp(FileIndex.modifytime)
                finfo.IsReadOnly = FileIndex.readonly
                finfo.LastAccessTimeUtc = TapeUtils.ParseTimeStamp(FileIndex.accesstime)
            End If
        Catch ex As Exception
            PrintMsg($"error:{My.Resources.ResText_OverwritingDF}{FileName} StackTrace {ex.StackTrace}", LogOnly:=True)
        End Try

        Threading.Interlocked.Increment(CurrentFilesProcessed)
        Threading.Interlocked.Increment(TotalFilesProcessed)

    End Sub
    Private Sub 提取ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 提取ToolStripMenuItem.Click
        If ListView1.SelectedItems IsNot Nothing AndAlso
        ListView1.SelectedItems.Count > 0 AndAlso
        FolderBrowserDialog1.ShowDialog() = DialogResult.OK Then
            Dim BasePath As String = FolderBrowserDialog1.SelectedPath
            LockGUI()
            Dim flist As New List(Of ltfsindex.file)
            For Each SI As ListViewItem In ListView1.SelectedItems
                If TypeOf SI.Tag Is ltfsindex.file Then
                    flist.Add(SI.Tag)
                End If
            Next

            Dim th As New Threading.Thread(
                    Sub()
                        Try
                            CurrentFilesProcessed = 0
                            CurrentBytesProcessed = 0
                            UnwrittenSizeOverrideValue = 0
                            UnwrittenCountOverwriteValue = flist.Count

                            For Each FI As ltfsindex.file In flist
                                UnwrittenSizeOverrideValue += FI.length
                                FI.TempObj = Nothing
                            Next
                            PrintMsg(My.Resources.ResText_Restoring)
                            StopFlag = False
                            TapeUtils.ReserveUnit(TapeDrive)
                            TapeUtils.PreventMediaRemoval(TapeDrive)
                            RestorePosition = New TapeUtils.PositionData(TapeDrive)
                            For Each FileIndex As ltfsindex.file In flist
                                Dim FileName As String = IO.Path.Combine(BasePath, FileIndex.name)
                                RestoreFile(FileName, FileIndex)
                                If StopFlag Then
                                    PrintMsg(My.Resources.ResText_OpCancelled)
                                    Exit Sub
                                End If
                            Next
                        Catch ex As Exception
                            PrintMsg($"{My.Resources.ResText_RestoreErr}{ex.ToString}", ForceLog:=True)
                        End Try
                        TapeUtils.AllowMediaRemoval(TapeDrive)
                        TapeUtils.ReleaseUnit(TapeDrive)
                        StopFlag = False
                        UnwrittenSizeOverrideValue = 0
                        UnwrittenCountOverwriteValue = 0
                        LockGUI(False)
                        PrintMsg(My.Resources.ResText_RestFin)
                        Invoke(Sub() MessageBox.Show(My.Resources.ResText_RestFin))
                    End Sub)
            th.Start()
        End If
    End Sub
    Private Sub 提取ToolStripMenuItem1_Click(sender As Object, e As EventArgs) Handles 提取ToolStripMenuItem1.Click
        If TreeView1.SelectedNode IsNot Nothing AndAlso FolderBrowserDialog1.ShowDialog = DialogResult.OK Then
            Dim th As New Threading.Thread(
                    Sub()
                        PrintMsg(My.Resources.ResText_Restoring)
                        Try
                            StopFlag = False
                            Dim FileList As New List(Of FileRecord)
                            Dim selectedDir As ltfsindex.directory = TreeView1.SelectedNode.Tag
                            Dim IterDir As Action(Of ltfsindex.directory, IO.DirectoryInfo) =
                                Sub(tapeDir As ltfsindex.directory, outputDir As IO.DirectoryInfo)
                                    For Each f As ltfsindex.file In tapeDir.contents._file
                                        f.TempObj = New ltfsindex.file.refFile() With {.FileName = ""}
                                        FileList.Add(New FileRecord With {.File = f, .SourcePath = IO.Path.Combine(outputDir.FullName, f.name)})
                                        'RestoreFile(IO.Path.Combine(outputDir.FullName, f.name), f)
                                    Next
                                    For Each d As ltfsindex.directory In tapeDir.contents._directory
                                        Dim thisDir As String = IO.Path.Combine(outputDir.FullName, d.name)
                                        Dim dirOutput As IO.DirectoryInfo
                                        Dim RestoreTimeStamp As Boolean = Not IO.Directory.Exists(thisDir)
                                        If RestoreTimeStamp Then IO.Directory.CreateDirectory(thisDir)
                                        dirOutput = New IO.DirectoryInfo(thisDir)
                                        IterDir(d, dirOutput)
                                        If RestoreTimeStamp Then
                                            dirOutput.CreationTimeUtc = TapeUtils.ParseTimeStamp(d.creationtime)
                                            dirOutput.LastWriteTimeUtc = TapeUtils.ParseTimeStamp(d.modifytime)
                                            dirOutput.LastAccessTimeUtc = TapeUtils.ParseTimeStamp(d.accesstime)
                                        End If
                                    Next
                                End Sub
                            PrintMsg(My.Resources.ResText_PrepFile)
                            Dim ODir As String = IO.Path.Combine(FolderBrowserDialog1.SelectedPath, selectedDir.name)
                            If Not ODir.StartsWith("\\") Then ODir = $"\\?\{ODir}"
                            If Not IO.Directory.Exists(ODir) Then IO.Directory.CreateDirectory(ODir)
                            IterDir(selectedDir, New IO.DirectoryInfo(ODir))
                            FileList.Sort(New Comparison(Of FileRecord)(Function(a As FileRecord, b As FileRecord) As Integer
                                                                            If a.File.extentinfo.Count = 0 And b.File.extentinfo.Count <> 0 Then Return 0.CompareTo(1)
                                                                            If b.File.extentinfo.Count = 0 And a.File.extentinfo.Count <> 0 Then Return 1.CompareTo(0)
                                                                            If a.File.extentinfo.Count = 0 And b.File.extentinfo.Count = 0 Then Return 0.CompareTo(0)
                                                                            If a.File.extentinfo(0).partition = ltfsindex.PartitionLabel.a And b.File.extentinfo(0).partition = ltfsindex.PartitionLabel.b Then Return 0.CompareTo(1)
                                                                            If a.File.extentinfo(0).partition = ltfsindex.PartitionLabel.b And b.File.extentinfo(0).partition = ltfsindex.PartitionLabel.a Then Return 1.CompareTo(0)
                                                                            Return a.File.extentinfo(0).startblock.CompareTo(b.File.extentinfo(0).startblock)
                                                                        End Function))
                            For i As Integer = 1 To FileList.Count - 1
                                If FileList(i).File.length = FileList(i - 1).File.length AndAlso FileList(i).File.sha1.Length = 40 AndAlso FileList(i).File.sha1 = FileList(i - 1).File.sha1 Then
                                    FileList(i).File.TempObj = FileList(i - 1).File.TempObj
                                End If
                            Next
                            CurrentFilesProcessed = 0
                            CurrentBytesProcessed = 0
                            UnwrittenSizeOverrideValue = 0
                            UnwrittenCountOverwriteValue = FileList.Count

                            For Each FI As FileRecord In FileList
                                UnwrittenSizeOverrideValue += FI.File.length
                                FI.File.TempObj = Nothing
                            Next
                            PrintMsg(My.Resources.ResText_RestFile)
                            Dim c As Integer = 0
                            TapeUtils.ReserveUnit(TapeDrive)
                            TapeUtils.PreventMediaRemoval(TapeDrive)
                            RestorePosition = New TapeUtils.PositionData(TapeDrive)
                            For Each fr As FileRecord In FileList
                                c += 1
                                PrintMsg($"{My.Resources.ResText_Restoring} [{c}/{FileList.Count}] {fr.File.name}", False, $"{My.Resources.ResText_Restoring} [{c}/{FileList.Count}] {fr.SourcePath}")
                                RestoreFile(fr.SourcePath, fr.File)
                                If StopFlag Then
                                    PrintMsg(My.Resources.ResText_OpCancelled)
                                    Exit Try
                                End If
                            Next
                            PrintMsg(My.Resources.ResText_RestFin)
                        Catch ex As Exception
                            Invoke(Sub() MessageBox.Show(ex.ToString))
                            PrintMsg($"{My.Resources.ResText_RestoreErr}{ex.ToString}", ForceLog:=True)
                        End Try
                        TapeUtils.AllowMediaRemoval(TapeDrive)
                        TapeUtils.ReleaseUnit(TapeDrive)
                        UnwrittenSizeOverrideValue = 0
                        UnwrittenCountOverwriteValue = 0
                        LockGUI(False)
                    End Sub)
            LockGUI()
            th.Start()
        End If
    End Sub
    Private Sub 删除ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 删除ToolStripMenuItem.Click
        DeleteDir()
    End Sub
    Private Sub 重命名ToolStripMenuItem_Click_1(sender As Object, e As EventArgs) Handles 重命名ToolStripMenuItem.Click
        RenameDir()
    End Sub
    Private Sub 更新索引ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 更新全部索引ToolStripMenuItem.Click
        Dim th As New Threading.Thread(
                Sub()
                    Try
                        UpdataAllIndex()
                    Catch ex As Exception
                        PrintMsg(My.Resources.ResText_IUErr, False, $"{My.Resources.ResText_IUErr}: {ex.ToString}")
                        LockGUI(False)
                    End Try
                End Sub)
        LockGUI(True)
        th.Start()
    End Sub
    Public Function LocateToWritePosition() As Boolean
        If schema.location.partition = ltfsindex.PartitionLabel.a Then
            TapeUtils.Locate(TapeDrive, schema.previousgenerationlocation.startblock, schema.previousgenerationlocation.partition, TapeUtils.LocateDestType.Block)
            schema.location.startblock = schema.previousgenerationlocation.startblock
            schema.location.partition = schema.previousgenerationlocation.partition
            Dim p As TapeUtils.PositionData = GetPos
            PrintMsg($"Position = {p.ToString()}", LogOnly:=True)
            PrintMsg(My.Resources.ResText_RI)
            Dim tmpf As String = $"{Application.StartupPath}\LWS_{Now.ToString("yyyyMMdd_HHmmss.fffffff")}.tmp"
            TapeUtils.ReadToFileMark(TapeDrive, tmpf, plabel.blocksize)
            PrintMsg(My.Resources.ResText_AI)
            'Dim sch2 As ltfsindex = ltfsindex.FromSchemaText(Encoding.UTF8.GetString(schraw))
            Dim sch2 As ltfsindex = ltfsindex.FromSchFile(tmpf)
            IO.File.Delete(tmpf)
            PrintMsg(My.Resources.ResText_AISucc)
            schema.previousgenerationlocation = sch2.previousgenerationlocation
            p = GetPos
            PrintMsg($"Position = {p.ToString()}", LogOnly:=True)
            CurrentHeight = p.BlockNumber
            DirProvider.UpdateCurrentHeight(GetSqliteConnection(Barcode), Barcode, CurrentHeight)
            Invoke(Sub() Text = GetLocInfo())
        ElseIf CurrentHeight > 0 Then
            Dim p As TapeUtils.PositionData = GetPos
            PrintMsg($"Position = {p.ToString()}", LogOnly:=True)
            If p.BlockNumber <> CurrentHeight Then
                TapeUtils.Locate(TapeDrive, CurrentHeight, DataPartition, TapeUtils.LocateDestType.Block)
                p = GetPos
                PrintMsg($"Position = {p.ToString()}", LogOnly:=True)
            End If
        Else
            Dim p As TapeUtils.PositionData = GetPos
            If MessageBox.Show($"{My.Resources.ResText_CurPos}P{p.PartitionNumber} B{p.BlockNumber}{My.Resources.ResText_NHWrn}", My.Resources.ResText_WriteWarning, MessageBoxButtons.OKCancel) = DialogResult.OK Then

            Else
                LockGUI(False)
                Return False
            End If
        End If
        Return True
    End Function
    Private Sub 写入数据ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 写入数据ToolStripMenuItem.Click
        Dim th As New Threading.Thread(
            Sub()
                Dim OnWriteFinishMessage As String = ""
                Try
                    Dim StartTime As Date = Now
                    PrintMsg("", True)
                    PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                    PrintMsg(My.Resources.ResText_PrepW)
                    TapeUtils.ReserveUnit(TapeDrive)
                    TapeUtils.PreventMediaRemoval(TapeDrive)
                    If Not LocateToWritePosition() Then Exit Sub
                    Invoke(Sub() 更新数据区索引ToolStripMenuItem.Enabled = True)
                    UFReadCount.Inc()
                    CurrentFilesProcessed = 0
                    CurrentBytesProcessed = 0
                    UnwrittenSizeOverrideValue = 0
                    UnwrittenCountOverwriteValue = 0

                    If UnwrittenFiles.Count > 0 Then
                        Dim WriteList As New List(Of FileRecord)
                        UFReadCount.Inc()
                        For Each fr As FileRecord In UnwrittenFiles
                            WriteList.Add(fr)
                        Next
                        UFReadCount.Dec()
                        UnwrittenCountOverwriteValue = UnwrittenCount
                        UnwrittenSizeOverrideValue = UnwrittenSize
                        Dim wBufferPtr As IntPtr = Marshal.AllocHGlobal(plabel.blocksize)

                        Dim PNum As Integer = My.Settings.LTFSWriter_PreLoadNum
                        If PNum > 0 Then
                            For j As Integer = 0 To PNum
                                If j < WriteList.Count Then WriteList(j).BeginOpen(BlockSize:=plabel.blocksize)
                            Next
                        End If
                        Dim HashTaskAwaitNumber As Integer = 0
                        Threading.ThreadPool.SetMaxThreads(1024, 1024)
                        Threading.ThreadPool.SetMinThreads(256, 256)
                        Dim ExitForFlag As Boolean = False
                        'DeDupe

                        Dim AllFile As New List(Of ltfsindex.file)
                        If My.Settings.LTFSWriter_DeDupe Then
                            Dim q As New List(Of ltfsindex.directory)
                            For Each d As ltfsindex.directory In schema._directory
                                q.Add(d)
                            Next
                            For Each f As ltfsindex.file In schema._file
                                AllFile.Add(f)
                            Next
                            While q.Count > 0
                                Dim q2 As New List(Of ltfsindex.directory)
                                For Each d As ltfsindex.directory In q
                                    For Each f As ltfsindex.file In d.contents._file
                                        AllFile.Add(f)
                                    Next
                                    For Each d2 As ltfsindex.directory In d.contents._directory
                                        q2.Add(d2)
                                    Next
                                Next
                                q = q2
                            End While
                        End If

                        Dim p As New TapeUtils.PositionData(TapeDrive)
                        TapeUtils.SetBlockSize(TapeDrive, plabel.blocksize)
                        For i As Integer = 0 To WriteList.Count - 1
                            If i < WriteList.Count - 1 Then
                                Dim CFNum As Integer = i
                                Dim dl As New LTFSWriter.FileRecord.PreReadFinishedEventHandler(
                                    Sub()
                                        Metric.PreReadCounter.WithLabels(Barcode).Inc()
                                        WriteList(CFNum + 1).BeginOpen()
                                    End Sub)
                                AddHandler WriteList(CFNum).PreReadFinished, dl
                            End If
                            If ExitForFlag Then Exit For
                            PNum = My.Settings.LTFSWriter_PreLoadNum
                            If PNum > 0 AndAlso i + PNum < WriteList.Count Then
                                WriteList(i + PNum).BeginOpen(BlockSize:=plabel.blocksize)
                            End If
                            Dim fr As FileRecord = WriteList(i)
                            Try
                                Dim finfo As IO.FileInfo = New IO.FileInfo(fr.SourcePath)
                                fr.File.fileuid = schema.highestfileuid + 1
                                schema.highestfileuid += 1
                                If finfo.Length > 0 Then
                                    'p = New TapeUtils.PositionData(TapeDrive)
                                    'If p.EOP Then PrintMsg(My.Resources.ResText_EWEOM.Text, True)
                                    Dim dupe As Boolean = False
                                    Dim minLength = My.Settings.LTFSWriter_DeDuplicate_MinLength
                                    If My.Settings.LTFSWriter_DeDupe AndAlso finfo.Length >= minLength Then
                                        Dim dupeFile As ltfsindex.file = Nothing
                                        Dim sha1value As String = ""
                                        '补索引里的sha1
                                        For Each fref As ltfsindex.file In AllFile
                                            If fref.length = finfo.Length AndAlso fref.sha1 <> "" Then
                                                PrintMsg($"{My.Resources.ResText_CHashing}: {fr.File.name}  {My.Resources.ResText_Size} {IOManager.FormatSize(fr.File.length)}", LogOnly:=True)
                                                If sha1value = "" Then sha1value = IOManager.SHA1(fr.SourcePath)
                                                If fref.sha1.Equals(sha1value) Then
                                                    fr.File.sha1 = sha1value
                                                    dupe = True
                                                    dupeFile = fref
                                                End If
                                            End If
                                            If dupe Then Exit For
                                        Next

                                        If dupe AndAlso dupeFile IsNot Nothing Then
                                            For Each ext As ltfsindex.file.extent In dupeFile.extentinfo
                                                fr.File.extentinfo.Add(ext)
                                            Next
                                            If fr.fs IsNot Nothing Then fr.Close()
                                            PrintMsg($"{My.Resources.ResText_Skip} {fr.File.name}  {My.Resources.ResText_Size} {IOManager.FormatSize(fr.File.length)}", False,
                                                 $"{My.Resources.ResText_Skip}: {fr.SourcePath}{vbCrLf}{My.Resources.ResText_Size}: {IOManager.FormatSize(fr.File.length)}{vbCrLf _
                                                 }{My.Resources.ResText_WrittenTotal}: {IOManager.FormatSize(TotalBytesProcessed) _
                                                 } {My.Resources.ResText_Remaining}: {IOManager.FormatSize(Math.Max(0, UnwrittenSize - CurrentBytesProcessed)) _
                                                 } -> {IOManager.FormatSize(Math.Max(0, UnwrittenSize - CurrentBytesProcessed - fr.File.length))}")
                                            TotalBytesProcessed += finfo.Length
                                            CurrentBytesProcessed += finfo.Length
                                            TotalFilesProcessed += 1
                                            CurrentFilesProcessed += 1

                                            'TotalBytesUnindexed += finfo.Length
                                        Else
                                            AllFile.Add(fr.File)
                                        End If
                                    End If
                                    If Not dupe Then
                                        Dim fileextent As New ltfsindex.file.extent With
                                            {.partition = ltfsindex.PartitionLabel.b,
                                            .startblock = p.BlockNumber,
                                            .bytecount = finfo.Length,
                                            .byteoffset = 0,
                                            .fileoffset = 0}
                                        fr.File.extentinfo.Add(fileextent)
                                        PrintMsg($"{My.Resources.ResText_Writing} {fr.File.name}  {My.Resources.ResText_Size} {IOManager.FormatSize(fr.File.length)}", False,
                                             $"{My.Resources.ResText_Writing}: {fr.SourcePath}{vbCrLf}{My.Resources.ResText_Size}: {IOManager.FormatSize(fr.File.length)}{vbCrLf _
                                             }{My.Resources.ResText_WrittenTotal}: {IOManager.FormatSize(TotalBytesProcessed) _
                                             } {My.Resources.ResText_Remaining}: {IOManager.FormatSize(Math.Max(0, UnwrittenSize - CurrentBytesProcessed)) _
                                             } -> {IOManager.FormatSize(Math.Max(0, UnwrittenSize - CurrentBytesProcessed - fr.File.length))}", LogOnly:=True)
                                        'write to tape
                                        If finfo.Length <= plabel.blocksize Then
                                            Dim succ As Boolean = False
                                            Dim startTimestamp = DateTime.Now
                                            Dim FileData As Byte()
                                            While True
                                                Try
                                                    FileData = fr.ReadAllBytes()
                                                    Exit While
                                                Catch ex As Exception
                                                    Select Case MessageBox.Show($"{My.Resources.ResText_WErr }{vbCrLf}{ex.ToString}", My.Resources.ResText_Warning, MessageBoxButtons.AbortRetryIgnore)
                                                        Case DialogResult.Abort
                                                            Throw ex
                                                        Case DialogResult.Retry

                                                        Case DialogResult.Ignore
                                                            PrintMsg($"Cannot read file {fr.SourcePath}", LogOnly:=True, ForceLog:=True)
                                                            Continue For
                                                    End Select
                                                End Try
                                            End While
                                            Metric.FileOperationDurationHistogram.WithLabels(Barcode, "read_disk", "small").Observe((DateTime.Now - startTimestamp).TotalMilliseconds)
                                            Metric.FileOperationDurationSummary.WithLabels(Barcode, "read_disk", "small").Observe((DateTime.Now - startTimestamp).TotalMilliseconds)
                                            While Not succ
                                                Dim sense As Byte()
                                                Try
                                                    sense = TapeUtils.Write(TapeDrive, FileData)
                                                    SyncLock p
                                                        p.BlockNumber += 1
                                                    End SyncLock
                                                Catch ex As Exception
                                                    PrintMsg($"{My.Resources.ResText_WErrSCSI}{My.Resources.ResText_Warning} {ex.Message}")
                                                    Select Case MessageBox.Show(My.Resources.ResText_WErrSCSI, My.Resources.ResText_Warning, MessageBoxButtons.AbortRetryIgnore)
                                                        Case DialogResult.Abort
                                                            Throw ex
                                                        Case DialogResult.Retry
                                                            succ = False
                                                        Case DialogResult.Ignore
                                                            succ = True
                                                            Exit While
                                                    End Select
                                                    p = New TapeUtils.PositionData(TapeDrive)
                                                    Continue While
                                                End Try
                                                If ((sense(2) >> 6) And &H1) = 1 Then
                                                    If (sense(2) And &HF) = 13 Then
                                                        PrintMsg(My.Resources.ResText_VOF)
                                                        Invoke(Sub() MessageBox.Show(My.Resources.ResText_VOF))
                                                        StopFlag = True
                                                        '磁带已满
                                                        Exit For
                                                    Else
                                                        PrintMsg(My.Resources.ResText_EWEOM, True)
                                                        succ = True
                                                        '磁带即将写满
                                                        Exit While
                                                    End If
                                                ElseIf sense(2) And &HF <> 0 Then
                                                    '写入出错
                                                    PrintMsg($"sense err {TapeUtils.Byte2Hex(sense, True)}", Warning:=True, LogOnly:=True)
                                                    Select Case MessageBox.Show($"{My.Resources.ResText_WErr}{vbCrLf}{TapeUtils.ParseSenseData(sense)}{vbCrLf}{vbCrLf}sense{vbCrLf}{TapeUtils.Byte2Hex(sense, True)}", My.Resources.ResText_Warning, MessageBoxButtons.AbortRetryIgnore)
                                                        Case DialogResult.Abort
                                                            Throw New Exception(TapeUtils.ParseSenseData(sense))
                                                        Case DialogResult.Retry
                                                            succ = False
                                                        Case DialogResult.Ignore
                                                            succ = True
                                                            Exit While
                                                    End Select
                                                    p = New TapeUtils.PositionData(TapeDrive)
                                                Else
                                                    succ = True
                                                End If
                                            End While
                                            If succ AndAlso HashOnWrite Then
                                                Task.Run(Sub()
                                                             Threading.Interlocked.Increment(HashTaskAwaitNumber)
                                                             Dim sh As New IOManager.CheckSumBlockwiseCalculator
                                                             sh.Propagate(FileData)
                                                             sh.ProcessFinalBlock()
                                                             fr.File.SetXattr(ltfsindex.file.xattr.HashType.SHA1, sh.SHA1Value)
                                                             fr.File.SetXattr(ltfsindex.file.xattr.HashType.MD5, sh.MD5Value)
                                                             Threading.Interlocked.Decrement(HashTaskAwaitNumber)
                                                         End Sub)
                                            End If
                                            If fr.fs IsNot Nothing Then fr.Close()
                                            fr.File.WrittenBytes += finfo.Length
                                            TotalBytesProcessed += finfo.Length
                                            CurrentBytesProcessed += finfo.Length
                                            TotalFilesProcessed += 1
                                            CurrentFilesProcessed += 1

                                            TotalBytesUnindexed += finfo.Length
                                        Else
                                            'Dim fs As New IO.FileStream(fr.SourcePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read, 512, True)
                                            Dim startTimestamp1 = DateTime.Now
                                            Select Case fr.Open()
                                                Case DialogResult.Ignore
                                                    PrintMsg($"Cannot open file {fr.SourcePath}", LogOnly:=True, ForceLog:=True)
                                                    Continue For
                                                Case DialogResult.Abort
                                                    Throw New Exception(My.Resources.ResText_FileOpenError)
                                            End Select
                                            Dim duration1 As TimeSpan = DateTime.Now - startTimestamp1
                                            Metric.FileOperationDurationHistogram.WithLabels(Barcode, "open_disk_file", "big").Observe(duration1.TotalMilliseconds)
                                            Metric.FileOperationDurationSummary.WithLabels(Barcode, "open_disk_file", "big").Observe(duration1.TotalMilliseconds)

                                            'PrintMsg($"File Opened:{fr.SourcePath}", LogOnly:=True)
                                            Dim sh As IOManager.CheckSumBlockwiseCalculator = Nothing
                                            If HashOnWrite Then sh = New IOManager.CheckSumBlockwiseCalculator
                                            Dim LastWriteTask As Task = Nothing
                                            Dim ExitWhileFlag As Boolean = False
                                            'Dim tstart As Date = Now
                                            'Dim tsub As Double = 0
                                            While Not StopFlag
                                                Dim buffer(plabel.blocksize - 1) As Byte
                                                Dim startTimestamp = DateTime.Now
                                                Dim BytesReaded As Integer = fr.Read(buffer, 0, plabel.blocksize)
                                                Dim duration As TimeSpan = DateTime.Now - startTimestamp
                                                Metric.FileOperationDurationHistogram.WithLabels(Barcode, "read_disk", "big").Observe(duration1.TotalMilliseconds)
                                                Metric.FileOperationDurationSummary.WithLabels(Barcode, "read_disk", "big").Observe(duration1.TotalMilliseconds)
                                                If LastWriteTask IsNot Nothing Then LastWriteTask.Wait()
                                                If ExitWhileFlag Then Exit While
                                                LastWriteTask = Task.Run(
                                                Sub()
                                                    If BytesReaded > 0 Then
                                                        CheckCount += 1
                                                        If CheckCount >= CheckCycle Then CheckCount = 0
                                                        If SpeedLimit > 0 AndAlso CheckCount = 0 Then
                                                            Dim ts As Double = (Now - SpeedLimitLastTriggerTime).TotalSeconds
                                                            While SpeedLimit > 0 AndAlso ts > 0 AndAlso ((plabel.blocksize * CheckCycle / 1048576) / ts) > SpeedLimit
                                                                Threading.Thread.Sleep(0)
                                                                ts = (Now - SpeedLimitLastTriggerTime).TotalSeconds
                                                            End While
                                                            SpeedLimitLastTriggerTime = Now
                                                        End If
                                                        Marshal.Copy(buffer, 0, wBufferPtr, BytesReaded)
                                                        Dim succ As Boolean = False
                                                        While Not succ
                                                            Dim sense As Byte()
                                                            Try
                                                                'Dim t0 As Date = Now
                                                                Dim startTimestamp2 = DateTime.Now
                                                                sense = TapeUtils.Write(TapeDrive, wBufferPtr, BytesReaded, True)
                                                                Dim duration2 As TimeSpan = DateTime.Now - startTimestamp2
                                                                Metric.FileOperationDurationHistogram.WithLabels(Barcode, "write_tape", "big").Observe(duration2.TotalMilliseconds)
                                                                Metric.FileOperationDurationSummary.WithLabels(Barcode, "write_tape", "big").Observe(duration2.TotalMilliseconds)
                                                                'tsub += (Now - t0).TotalMilliseconds
                                                                'Invoke(Sub() Text = tsub / (Now - tstart).TotalMilliseconds)
                                                                SyncLock p
                                                                    p.BlockNumber += 1
                                                                End SyncLock
                                                            Catch ex As Exception
                                                                Select Case MessageBox.Show(My.Resources.ResText_WErrSCSI, My.Resources.ResText_Warning, MessageBoxButtons.AbortRetryIgnore)
                                                                    Case DialogResult.Abort
                                                                        Throw ex
                                                                    Case DialogResult.Retry
                                                                        succ = False
                                                                    Case DialogResult.Ignore
                                                                        succ = True
                                                                        Exit While
                                                                End Select
                                                                p = New TapeUtils.PositionData(TapeDrive)
                                                                Continue While
                                                            End Try
                                                            If (((sense(2) >> 6) And &H1) = 1) Then
                                                                If ((sense(2) And &HF) = 13) Then
                                                                    PrintMsg(My.Resources.ResText_VOF)
                                                                    Invoke(Sub() MessageBox.Show(My.Resources.ResText_VOF))
                                                                    StopFlag = True
                                                                    fr.Close()
                                                                    ExitForFlag = True
                                                                    Exit Sub
                                                                Else
                                                                    PrintMsg(My.Resources.ResText_EWEOM, True)
                                                                    succ = True
                                                                    Exit While
                                                                End If
                                                            ElseIf sense(2) And &HF <> 0 Then
                                                                Select Case MessageBox.Show($"{My.Resources.ResText_WErr}{vbCrLf}{TapeUtils.ParseSenseData(sense)}{vbCrLf}{vbCrLf}sense{vbCrLf}{TapeUtils.Byte2Hex(sense, True)}", My.Resources.ResText_Warning, MessageBoxButtons.AbortRetryIgnore)
                                                                    Case DialogResult.Abort
                                                                        Throw New Exception(TapeUtils.ParseSenseData(sense))
                                                                    Case DialogResult.Retry
                                                                        succ = False
                                                                    Case DialogResult.Ignore
                                                                        succ = True
                                                                        Exit While
                                                                End Select
                                                                p = New TapeUtils.PositionData(TapeDrive)
                                                            Else
                                                                succ = True
                                                                Exit While
                                                            End If
                                                        End While
                                                        If sh IsNot Nothing AndAlso succ Then
                                                            Dim startTimestamp2 = DateTime.Now
                                                            If 异步校验CPU占用高ToolStripMenuItem.Checked Then

                                                                sh.PropagateAsync(buffer, BytesReaded)
                                                                Dim duration2 As TimeSpan = DateTime.Now - startTimestamp2
                                                                Metric.FileOperationDurationHistogram.WithLabels(Barcode, "async_sha", "big").Observe(duration2.TotalMilliseconds)
                                                                Metric.FileOperationDurationSummary.WithLabels(Barcode, "async_sha", "big").Observe(duration2.TotalMilliseconds)
                                                            Else
                                                                sh.Propagate(buffer, BytesReaded)
                                                                Dim duration2 As TimeSpan = DateTime.Now - startTimestamp2
                                                                Metric.FileOperationDurationHistogram.WithLabels(Barcode, "sync_sha", "big").Observe(duration2.TotalMilliseconds)
                                                                Metric.FileOperationDurationSummary.WithLabels(Barcode, "sync_sha", "big").Observe(duration2.TotalMilliseconds)
                                                            End If
                                                        End If
                                                        Dim startTimestamp3 = DateTime.Now
                                                        If Flush Then CheckFlush()
                                                        Dim duration3 As TimeSpan = DateTime.Now - startTimestamp3
                                                        Metric.FileOperationDurationHistogram.WithLabels(Barcode, "CheckFlush", "big").Observe(duration3.TotalMilliseconds)
                                                        Metric.FileOperationDurationSummary.WithLabels(Barcode, "CheckFlush", "big").Observe(duration3.TotalMilliseconds)
                                                        Dim startTimestamp4 = DateTime.Now
                                                        If Clean Then CheckClean(True)
                                                        Dim duration4 As TimeSpan = DateTime.Now - startTimestamp4
                                                        Metric.FileOperationDurationHistogram.WithLabels(Barcode, "CheckClean", "big").Observe(duration4.TotalMilliseconds)
                                                        Metric.FileOperationDurationSummary.WithLabels(Barcode, "CheckClean", "big").Observe(duration4.TotalMilliseconds)
                                                        fr.File.WrittenBytes += BytesReaded
                                                        TotalBytesProcessed += BytesReaded
                                                        CurrentBytesProcessed += BytesReaded
                                                        TotalBytesUnindexed += BytesReaded
                                                    Else
                                                        ExitWhileFlag = True
                                                    End If
                                                End Sub)
                                            End While
                                            Dim startTimestamp5 = DateTime.Now
                                            If LastWriteTask IsNot Nothing Then LastWriteTask.Wait()
                                            Dim duration5 As TimeSpan = DateTime.Now - startTimestamp5
                                            Metric.FileOperationDurationHistogram.WithLabels(Barcode, "LastWriteTask.Wait()", "big").Observe(duration5.TotalMilliseconds)
                                            Metric.FileOperationDurationSummary.WithLabels(Barcode, "LastWriteTask.Wait()", "big").Observe(duration5.TotalMilliseconds)

                                            fr.CloseAsync()
                                            If HashOnWrite AndAlso sh IsNot Nothing AndAlso Not StopFlag Then
                                                Threading.Interlocked.Increment(HashTaskAwaitNumber)
                                                Task.Run(Sub()
                                                             sh.ProcessFinalBlock()
                                                             fr.File.SetXattr(ltfsindex.file.xattr.HashType.SHA1, sh.SHA1Value)
                                                             fr.File.SetXattr(ltfsindex.file.xattr.HashType.MD5, sh.MD5Value)
                                                             sh.StopFlag = True
                                                             Threading.Interlocked.Decrement(HashTaskAwaitNumber)
                                                         End Sub)
                                            ElseIf sh IsNot Nothing Then
                                                sh.StopFlag = True
                                            End If
                                            TotalFilesProcessed += 1
                                            CurrentFilesProcessed += 1

                                        End If
                                        p = GetPos
                                        If p.EOP Then PrintMsg(My.Resources.ResText_EWEOM, True)
                                        PrintMsg($"Position = {p.ToString()}", LogOnly:=True)
                                        CurrentHeight = p.BlockNumber
                                        DirProvider.UpdateCurrentHeight(GetSqliteConnection(Barcode), Barcode, CurrentHeight)
                                    End If
                                Else
                                    fr.File.SetXattr(ltfsindex.file.xattr.HashType.SHA1, "DA39A3EE5E6B4B0D3255BFEF95601890AFD80709")
                                    fr.File.SetXattr(ltfsindex.file.xattr.HashType.MD5, "D41D8CD98F00B204E9800998ECF8427E")
                                    TotalBytesUnindexed += 1
                                    TotalFilesProcessed += 1
                                    CurrentFilesProcessed += 1

                                End If
                                'mark as written
                                fr.ParentDirectory.contents._file.Add(fr.File)
                                fr.ParentDirectory.contents.UnwrittenFiles.Remove(fr.File)
                                If TotalBytesUnindexed = 0 Then TotalBytesUnindexed = 1
                                If CheckUnindexedDataSizeLimit() Then p = New TapeUtils.PositionData(TapeDrive)
                                Invoke(Sub()
                                           If CapacityRefreshInterval > 0 AndAlso (Now - LastRefresh).TotalSeconds > CapacityRefreshInterval Then
                                               p = New TapeUtils.PositionData(TapeDrive)
                                               Dim startTimestamp4 = DateTime.Now
                                               RefreshCapacity()
                                               Dim duration4 As TimeSpan = DateTime.Now - startTimestamp4
                                               Metric.FileOperationDurationHistogram.WithLabels(Barcode, "RefreshCapacity", "big").Observe(duration4.TotalMilliseconds)
                                               Metric.FileOperationDurationSummary.WithLabels(Barcode, "RefreshCapacity", "big").Observe(duration4.TotalMilliseconds)

                                               Dim p2 As New TapeUtils.PositionData(TapeDrive)
                                               If p2.BlockNumber <> p.BlockNumber OrElse p2.PartitionNumber <> p.PartitionNumber Then
                                                   If MessageBox.Show($"Position changed! {p.BlockNumber} -> {p2.BlockNumber}", "Warning", MessageBoxButtons.OKCancel) = DialogResult.Cancel Then
                                                       StopFlag = True
                                                   End If
                                               End If
                                           End If
                                       End Sub)
                            Catch ex As Exception
                                MessageBox.Show($"{My.Resources.ResText_WErr}{ex.ToString}")
                                PrintMsg($"{My.Resources.ResText_WErr}{ex.Message}")
                            End Try
                            While Pause
                                Threading.Thread.Sleep(10)
                            End While
                            If StopFlag Then
                                Exit For
                            End If
                            UnwrittenFiles.Remove(fr)
                            WriteList(i) = Nothing
                        Next
                        Marshal.FreeHGlobal(wBufferPtr)
                        While HashTaskAwaitNumber > 0
                            Threading.Thread.Sleep(1)
                        End While
                    End If

                    UFReadCount.Dec()
                    Me.Invoke(Sub() Timer1_Tick(sender, e))
                    Dim TotalBytesWritten As Long = UnwrittenSizeOverrideValue
                    While True
                        Threading.Thread.Sleep(0)
                        SyncLock UFReadCount
                            If UFReadCount > 0 Then Continue While
                            UnwrittenFiles.Clear()
                            UnwrittenSizeOverrideValue = 0
                            UnwrittenCountOverwriteValue = 0
                            CurrentFilesProcessed = 0
                            CurrentBytesProcessed = 0
                            Exit While
                        End SyncLock
                    End While
                    Modified = True
                    If Not StopFlag Then
                        Dim TimeCost As TimeSpan = Now - StartTime
                        OnWriteFinishMessage = ($"{My.Resources.ResText_WFTime}{(Math.Floor(TimeCost.TotalHours)).ToString().PadLeft(2, "0")}:{TimeCost.Minutes.ToString().PadLeft(2, "0")}:{TimeCost.Seconds.ToString().PadLeft(2, "0")} {My.Resources.ResText_AvgS}{IOManager.FormatSize(TotalBytesWritten \ Math.Max(1, TimeCost.TotalSeconds))}/s")
                        OnWriteFinished()
                    Else
                        OnWriteFinishMessage = (My.Resources.ResText_WCnd)
                    End If
                Catch ex As Exception
                    MessageBox.Show($"{My.Resources.ResText_WErr}{ex.ToString}")
                    PrintMsg($"{My.Resources.ResText_WErr}{ex.Message}")
                End Try
                TapeUtils.Flush(TapeDrive)
                TapeUtils.ReleaseUnit(TapeDrive)
                TapeUtils.AllowMediaRemoval(TapeDrive)
                Invoke(Sub()
                           LockGUI(False)
                           RefreshDisplay()
                           RefreshCapacity()
                           If Not StopFlag AndAlso WA0ToolStripMenuItem.Checked AndAlso MessageBox.Show(My.Resources.ResText_WFUp, My.Resources.ResText_OpSucc, MessageBoxButtons.OKCancel) = DialogResult.OK Then
                               更新数据区索引ToolStripMenuItem_Click(sender, e)
                           End If
                           PrintMsg(OnWriteFinishMessage)
                           RaiseEvent WriteFinished()
                       End Sub)
            End Sub)
        StopFlag = False
        LockGUI()
        th.Start()
    End Sub
    Private Sub 清除当前索引后数据ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 清除当前索引后数据ToolStripMenuItem.Click

        If MessageBox.Show(My.Resources.ResText_X2, My.Resources.ResText_Warning, MessageBoxButtons.OKCancel) = DialogResult.Cancel Then
            Exit Sub
        End If
        Dim th As New Threading.Thread(
            Sub()
                Try
                    PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                    PrintMsg(My.Resources.ResText_Locating)
                    TapeUtils.Locate(TapeDrive, schema.location.startblock, schema.location.partition, TapeUtils.LocateDestType.Block)
                    PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                    PrintMsg(My.Resources.ResText_RI)
                    Dim outputfile As String = "schema\LTFSIndex_SetEOD_" & Now.ToString("yyyyMMdd_HHmmss.fffffff") & ".schema"
                    outputfile = IO.Path.Combine(Application.StartupPath, outputfile)
                    TapeUtils.ReadToFileMark(TapeDrive, outputfile, plabel.blocksize)
                    Dim CurrentPos As TapeUtils.PositionData = GetPos
                    PrintMsg($"Position = {CurrentPos.ToString()}", LogOnly:=True)
                    If CurrentPos.PartitionNumber < ExtraPartitionCount Then
                        Invoke(Sub() MessageBox.Show(My.Resources.ResText_IPCanc))
                        Exit Try
                    End If
                    TapeUtils.Locate(TapeDrive, CurrentPos.BlockNumber - 1, CurrentPos.PartitionNumber, TapeUtils.LocateDestType.Block)
                    PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                    TapeUtils.WriteFileMark(TapeDrive)
                    PrintMsg($"FileMark written", LogOnly:=True)
                    PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                    PrintMsg(My.Resources.ResText_AI)
                    schema = ltfsindex.FromSchFile(outputfile)
                    IO.File.Delete(outputfile)
                    PrintMsg(My.Resources.ResText_AISucc)
                    Modified = False
                    Dim p As TapeUtils.PositionData = GetPos
                    PrintMsg($"Position = {p.ToString()}", LogOnly:=True)
                    CurrentHeight = p.BlockNumber
                    If ExtraPartitionCount = 0 Then
                        TapeUtils.Write(TapeDrive, {0})
                        PrintMsg($"Byte written", LogOnly:=True)
                        PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                        TapeUtils.Locate(TapeDrive, CurrentHeight, 0, TapeUtils.LocateDestType.Block)
                        PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                    End If
                    While True
                        Threading.Thread.Sleep(0)
                        SyncLock UFReadCount
                            If UFReadCount > 0 Then Continue While
                            UnwrittenFiles.Clear()
                            CurrentFilesProcessed = 0
                            CurrentBytesProcessed = 0

                            Exit While
                        End SyncLock
                    End While
                    Me.Invoke(Sub()
                                  RefreshDisplay()
                                  RefreshCapacity()
                              End Sub)
                Catch ex As Exception
                    PrintMsg(ex.Message + vbCrLf + ex.StackTrace, LogOnly:=True)
                    PrintMsg(My.Resources.ResText_RFailed)
                End Try
                Modified = False
                PrintMsg(My.Resources.ResText_RollBacked)
                Me.Invoke(Sub()
                              LockGUI(False)
                              Text = GetLocInfo()
                          End Sub)
            End Sub)
        LockGUI()
        th.Start()
    End Sub
    Private Sub 启用日志记录ToolStripMenuItem_CheckedChanged(sender As Object, e As EventArgs) Handles 启用日志记录ToolStripMenuItem.CheckedChanged
        My.Settings.LTFSWriter_LogEnabled = 启用日志记录ToolStripMenuItem.Checked
    End Sub
    Private Sub 总是更新数据区索引ToolStripMenuItem_CheckedChanged(sender As Object, e As EventArgs) Handles 总是更新数据区索引ToolStripMenuItem.CheckedChanged
        My.Settings.LTFSWriter_ForceIndex = 总是更新数据区索引ToolStripMenuItem.Checked
    End Sub
    Private Sub 回滚ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 回滚ToolStripMenuItem.Click
        If MessageBox.Show($"{My.Resources.ResText_RB1}{schema.generationnumber}{My.Resources.ResText_RB2} {My.Resources.ResText_Partition}{schema.location.partition} {My.Resources.ResText_Block}{schema.location.startblock}{vbCrLf _
                           }{My.Resources.ResText_RB3} {My.Resources.ResText_Partition}{schema.previousgenerationlocation.partition} {My.Resources.ResText_Block}{schema.previousgenerationlocation.startblock}{vbCrLf _
                           }{My.Resources.ResText_RB4}", My.Resources.ResText_Warning, MessageBoxButtons.OKCancel) = DialogResult.Cancel Then
            Exit Sub
        End If
        Dim th As New Threading.Thread(
            Sub()
                Try
                    PrintMsg(My.Resources.ResText_RBing)
                    Dim genbefore As Integer = schema.generationnumber
                    Dim prevpart As ltfsindex.PartitionLabel = schema.previousgenerationlocation.partition
                    Dim prevblk As Long = schema.previousgenerationlocation.startblock
                    PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                    TapeUtils.Locate(TapeDrive, schema.previousgenerationlocation.startblock, schema.previousgenerationlocation.partition, TapeUtils.LocateDestType.Block)
                    PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                    PrintMsg(My.Resources.ResText_RI)
                    Dim outputfile As String = "schema\LTFSIndex_RollBack_" & Now.ToString("yyyyMMdd_HHmmss.fffffff") & ".schema"
                    outputfile = IO.Path.Combine(Application.StartupPath, outputfile)
                    TapeUtils.ReadToFileMark(TapeDrive, outputfile, plabel.blocksize)
                    PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                    PrintMsg(My.Resources.ResText_AI)
                    schema = ltfsindex.FromSchFile(outputfile)
                    PrintMsg(My.Resources.ResText_AISucc)
                    Modified = False
                    Dim p As TapeUtils.PositionData = GetPos
                    PrintMsg($"Position = {p.ToString()}", LogOnly:=True)
                    CurrentHeight = p.BlockNumber
                    While True
                        Threading.Thread.Sleep(0)
                        SyncLock UFReadCount
                            If UFReadCount > 0 Then Continue While
                            UnwrittenFiles.Clear()
                            CurrentFilesProcessed = 0
                            CurrentBytesProcessed = 0

                            Exit While
                        End SyncLock
                    End While
                    Me.Invoke(Sub()
                                  PrintMsg($"gen{genbefore}->{schema.generationnumber}: p{prevpart} block{prevblk}->p{schema.location.partition} block{schema.location.startblock}")
                                  RefreshDisplay()
                                  RefreshCapacity()
                              End Sub)
                Catch ex As Exception
                    PrintMsg(ex.Message + vbCrLf + ex.StackTrace, LogOnly:=True)
                    PrintMsg(My.Resources.ResText_RFailed)
                End Try
                Me.Invoke(Sub()
                              LockGUI(False)
                              Text = GetLocInfo()
                              PrintMsg(My.Resources.ResText_RBFin)
                          End Sub)
            End Sub)
        LockGUI()
        th.Start()
    End Sub
    Private Sub 读取索引ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 读取索引ToolStripMenuItem.Click
        Dim th As New Threading.Thread(
            Sub()
                Try
                    PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                    PrintMsg(My.Resources.ResText_Locating)
                    ExtraPartitionCount = TapeUtils.ModeSense(TapeDrive, &H11)(3)
                    TapeUtils.GlobalBlockLimit = TapeUtils.ReadBlockLimits(TapeDrive).MaximumBlockLength
                    If IO.File.Exists(IO.Path.Combine(Application.StartupPath, "blocklen.ini")) Then
                        Dim blval As Integer = Integer.Parse(IO.File.ReadAllText(IO.Path.Combine(Application.StartupPath, "blocklen.ini")))
                        If blval > 0 Then TapeUtils.GlobalBlockLimit = blval
                    End If
                    Barcode = TapeUtils.ReadBarcode(TapeDrive)
                    PrintMsg($"Barcode = {Barcode}", LogOnly:=True)
                    If False Then
                        '没变要每次到开头加载
                    TapeUtils.Locate(TapeDrive, 0, Math.Min(ExtraPartitionCount, IndexPartition), TapeUtils.LocateDestType.Block)
                    PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                    Dim header As String = Encoding.ASCII.GetString(TapeUtils.ReadBlock(TapeDrive))
                    PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                    Dim VOL1LabelLegal As Boolean = False
                    VOL1LabelLegal = (header.Length = 80)
                    If VOL1LabelLegal Then VOL1LabelLegal = header.StartsWith("VOL1")
                    If VOL1LabelLegal Then VOL1LabelLegal = (header.Substring(24, 4) = "LTFS")
                    If Not VOL1LabelLegal Then
                        PrintMsg(My.Resources.ResText_NVOL1)
                        Invoke(Sub() MessageBox.Show(My.Resources.ResText_NLTFS, My.Resources.ResText_Error))
                        LockGUI(False)
                        Exit Try
                    End If
                    TapeUtils.Locate(TapeDrive, 1, Math.Min(ExtraPartitionCount, IndexPartition), TapeUtils.LocateDestType.FileMark)
                    PrintMsg(My.Resources.ResText_RLTFSInfo)
                    PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                    TapeUtils.ReadFileMark(TapeDrive)
                    PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                    End If
                 
                    Dim cmData As New TapeUtils.CMParser(TapeDrive)
                    Dim TapeStatus = CType(cmData.g_CM(cmData.gtype.status), TapeUtils.CMParser.TapeStatus)
                    Dim plabelfile = $"plabel\\{Barcode}" & ".schema"
                    Dim plabelCountfile = $"plabel\\{Barcode}" & ".count"

                    If Not IO.Directory.Exists($"plabel") Then
                        IO.Directory.CreateDirectory($"plabel")
                    End If
                    Dim pltext As String
                    Dim plabelCount As Integer = 0
                    If IO.File.Exists(plabelCountfile) Then
                        Dim countFile = IO.File.ReadAllText(plabelCountfile)
                        plabelCount = Convert.ToInt32(countFile)
                    End If
                    DirProvider.CreateDatabaseAndTable(GetSqliteConnection(Barcode))
                    Dim ltfsIndexInfoDto = DirProvider.GetLTFSIndexInfo(GetSqliteConnection(Barcode), Barcode)
                    If ltfsIndexInfoDto Is Nothing Then
                        If (IO.File.Exists(plabelfile) And TapeStatus.ThreadCount <= plabelCount) And Not Barcode Is Nothing Then
                            pltext = IO.File.ReadAllText(plabelfile)
                        Else
                            TapeUtils.Locate(TapeDrive, 1, 0, TapeUtils.LocateDestType.FileMark)
                            PrintMsg(My.Resources.ResText_RLTFSInfo)
                            PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                            TapeUtils.ReadFileMark(TapeDrive)
                            pltext = Encoding.UTF8.GetString(TapeUtils.ReadToFileMark(TapeDrive))
                            IO.File.WriteAllText(plabelfile, pltext)
                            IO.File.WriteAllText(plabelCountfile, TapeStatus.ThreadCount.ToString)
                        End If
                        plabel = ltfslabel.FromXML(pltext)
                        TapeUtils.SetBlockSize(TapeDrive, plabel.blocksize)


                        If plabel.location.partition = plabel.partitions.data Then
                            DataPartition = GetPos().PartitionNumber
                            IndexPartition = (DataPartition + 1) Mod 2
                            If ExtraPartitionCount > 0 Then
                                IndexPartition = 255
                                PrintMsg($"Data partition detected. Switching to index partition", LogOnly:=True)
                                TapeUtils.Locate(TapeDrive, 1, IndexPartition, TapeUtils.LocateDestType.FileMark)
                                PrintMsg(My.Resources.ResText_RLTFSInfo)
                                PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                                TapeUtils.ReadFileMark(TapeDrive)
                                PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                                pltext = Encoding.UTF8.GetString(TapeUtils.ReadToFileMark(TapeDrive))
                                plabel = ltfslabel.FromXML(pltext)
                            End If
                        Else
                            IndexPartition = GetPos().PartitionNumber
                            DataPartition = (IndexPartition + 1) Mod 2
                        End If


                        PrintMsg(My.Resources.ResText_Locating)

                        If ExtraPartitionCount = 0 Then
                            IndexPartition = 255
                            TapeUtils.Locate(TapeDrive, 0, 0, TapeUtils.LocateDestType.EOD)
                            PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                            PrintMsg(My.Resources.ResText_RI)
                            If DisablePartition Then
                                TapeUtils.Space6(TapeDrive, -2, TapeUtils.LocateDestType.FileMark)
                            Else
                                Dim p As TapeUtils.PositionData = GetPos
                                Dim FM As Long = p.FileNumber
                                PrintMsg($"Position = {p.ToString()}", LogOnly:=True)
                                If FM <= 1 Then
                                    '索引读取失败
                                    PrintMsg(My.Resources.ResText_IRFailed)
                                    Invoke(Sub() MessageBox.Show(My.Resources.ResText_NLTFS, My.Resources.ResText_Error))
                                    LockGUI(False)
                                    Exit Try
                                End If
                                TapeUtils.Locate(TapeDrive, FM - 1, 0, TapeUtils.LocateDestType.FileMark)
                            End If
                            PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                            TapeUtils.ReadFileMark(TapeDrive)
                        Else
                            TapeUtils.Locate(TapeDrive, 3, IndexPartition, TapeUtils.LocateDestType.FileMark)
                            PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                            TapeUtils.ReadFileMark(TapeDrive)
                        End If
                        PrintMsg(My.Resources.ResText_RI)
                        PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                        Dim tmpf As String = $"{Application.StartupPath}\LCG_{Now.ToString("yyyyMMdd_HHmmss.fffffff")}.tmp"
                        '                    TapeUtils.Locate(TapeDrive, schema.previousgenerationlocation.startblock, schema.previousgenerationlocation.partition, TapeUtils.LocateDestType.Block)
                        TapeUtils.ReadToFileMark(TapeDrive, tmpf, plabel.blocksize)
                        PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                        PrintMsg(My.Resources.ResText_AI)
                        schema = ltfsindex.FromSchFile(tmpf)
                        If ExtraPartitionCount = 0 Then
                            Dim p As TapeUtils.PositionData = GetPos
                            PrintMsg($"Position = {p.ToString()}", LogOnly:=True)
                            CurrentHeight = p.BlockNumber
                        Else
                            CurrentHeight = -1
                        End If
                        PrintMsg(My.Resources.ResText_SvBak)
                        Dim FileName As String = ""
                        If Barcode <> "" Then
                            FileName = Barcode
                        Else
                            If schema IsNot Nothing Then
                                FileName = schema.volumeuuid.ToString()
                            End If
                        End If
                        If Not IO.Directory.Exists($"schema\{Barcode}") Then
                            IO.Directory.CreateDirectory($"schema\{Barcode}")
                        End If
                        Dim outputfile As String = $"schema\{Barcode}\LTFSIndex_Load_{FileName}_{Now.ToString("yyyyMMdd_HHmmss.fffffff")}.schema"
                        If Not IO.Directory.Exists(IO.Path.Combine(Application.StartupPath, "schema")) Then
                            IO.Directory.CreateDirectory(IO.Path.Combine(Application.StartupPath, "schema"))
                        End If
                        outputfile = IO.Path.Combine(Application.StartupPath, outputfile)
                        IO.File.Move(tmpf, outputfile)
                        DirProvider.InitializeLTFSIndexInfo(GetSqliteConnection(Barcode), plabel, schema, CurrentHeight, Barcode)
                    Else
                        schema = New ltfsindex With {
                           .creator = ltfsIndexInfoDto.LTFSIndex.creator,
                           .volumeuuid = ltfsIndexInfoDto.LTFSIndex.volumeuuid,
                          .generationnumber = ltfsIndexInfoDto.LTFSIndex.generationnumber,
                          .updatetime = ltfsIndexInfoDto.LTFSIndex.updatetime,
                          .location = New ltfsindex.PartitionDef With {
                          .partition = ltfsIndexInfoDto.LTFSIndex.location.partition,
                          .startblock = ltfsIndexInfoDto.LTFSIndex.location.startblock
                           },
                           .previousgenerationlocation = New ltfsindex.PartitionDef With {
                           .partition = ltfsIndexInfoDto.LTFSIndex.previousgenerationlocation.partition,
                           .startblock = ltfsIndexInfoDto.LTFSIndex.previousgenerationlocation.startblock
                           },
                           .highestfileuid = ltfsIndexInfoDto.LTFSIndex.highestfileuid}
                        CurrentHeight = ltfsIndexInfoDto.CurrentHeight
                        schema._directory.Add(New ltfsindex.directory With {.name = Barcode, .readonly = False,
                                                 .creationtime = plabel.formattime, .changetime = .creationtime,
                                                 .accesstime = .creationtime, .modifytime = .creationtime, .backuptime = .creationtime, .fileuid = 1, .contents = New ltfsindex.contentsDef()})
                    End If
                    While True
                        Threading.Thread.Sleep(0)
                        SyncLock UFReadCount
                            If UFReadCount > 0 Then Continue While
                            UnwrittenFiles.Clear()
                            CurrentFilesProcessed = 0
                            CurrentBytesProcessed = 0
                            TotalBytesUnindexed = 0

                            Exit While
                        End SyncLock
                    End While
                    Modified = False
                    Me.Invoke(Sub()
                                  MaxCapacity = 0
                                  Text = GetLocInfo()
                                  ToolStripStatusLabel1.Text = Barcode.TrimEnd(" ")
                                  ToolStripStatusLabel1.ToolTipText = $"{My.Resources.ResText_Barcode}:{ToolStripStatusLabel1.Text}{vbCrLf}{My.Resources.ResText_BlkSize}:{plabel.blocksize}"
                                  RefreshDisplay()
                                  RefreshCapacity()
                              End Sub)

                    PrintMsg(My.Resources.ResText_IRSucc)
                    LockGUI(False)
                    Invoke(Sub() RaiseEvent LTFSLoaded())
                Catch ex As Exception
                    PrintMsg(My.Resources.ResText_IRFailed)
                    PrintMsg(ex.ToString, LogOnly:=True)
                    LockGUI(False)
                End Try
            End Sub)
        LockGUI()
        th.Start()
    End Sub
    Private Sub 读取数据区索引ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 读取数据区索引ToolStripMenuItem.Click
        If ExtraPartitionCount = 0 Then
            读取索引ToolStripMenuItem_Click(sender, e)
            Exit Sub
        End If
        Dim th As New Threading.Thread(
            Sub()
                Try
                    PrintMsg(My.Resources.ResText_Locating)
                    Dim currentPos As TapeUtils.PositionData = GetPos
                    PrintMsg($"Position = {currentPos.ToString()}", LogOnly:=True)
                    If currentPos.PartitionNumber <> 1 Then TapeUtils.Locate(TapeDrive, 0, 1, TapeUtils.LocateDestType.Block)
                    TapeUtils.Locate(TapeDrive, 0, DataPartition, TapeUtils.LocateDestType.EOD)
                    PrintMsg(My.Resources.ResText_RI)
                    currentPos = GetPos
                    PrintMsg($"Position = {currentPos.ToString()}", LogOnly:=True)
                    If DisablePartition Then
                        TapeUtils.Space6(TapeDrive, -2, TapeUtils.LocateDestType.FileMark)
                    Else
                        Dim FM As Long = currentPos.FileNumber
                        If FM <= 1 Then
                            PrintMsg(My.Resources.ResText_IRFailed)
                            Invoke(Sub() MessageBox.Show(My.Resources.ResText_NLTFS, My.Resources.ResText_Error))
                            LockGUI(False)
                            Exit Try
                        End If
                        TapeUtils.Locate(TapeDrive, FM - 1, DataPartition, TapeUtils.LocateDestType.FileMark)
                    End If

                    TapeUtils.ReadFileMark(TapeDrive)
                    PrintMsg(My.Resources.ResText_RI)
                    Dim outputfile As String = "schema\LTFSIndex_LoadDPIndex_" & Now.ToString("yyyyMMdd_HHmmss.fffffff") & ".schema"
                    If Not IO.Directory.Exists(IO.Path.Combine(Application.StartupPath, "schema")) Then
                        IO.Directory.CreateDirectory(IO.Path.Combine(Application.StartupPath, "schema"))
                    End If
                    outputfile = IO.Path.Combine(Application.StartupPath, outputfile)
                    TapeUtils.ReadToFileMark(TapeDrive, outputfile, plabel.blocksize)
                    PrintMsg(My.Resources.ResText_AI)
                    schema = ltfsindex.FromSchFile(outputfile)
                    PrintMsg(My.Resources.ResText_AISucc)
                    
                    While True
                        Threading.Thread.Sleep(0)
                        SyncLock UFReadCount
                            If UFReadCount > 0 Then Continue While
                            UnwrittenFiles.Clear()
                            CurrentFilesProcessed = 0
                            CurrentBytesProcessed = 0
                            TotalBytesUnindexed = 0

                            TotalBytesUnindexed = 0
                            Exit While
                        End SyncLock
                    End While
                    Modified = False
                    Me.Invoke(Sub()
                                  MaxCapacity = 0
                                  ToolStripStatusLabel1.ToolTipText = ToolStripStatusLabel1.Text
                                  RefreshDisplay()
                                  RefreshCapacity()
                              End Sub)
                    CurrentHeight = -1
                    PrintMsg(My.Resources.ResText_IRSucc)
                Catch ex As Exception
                    PrintMsg(ex.Message+vbCrLf+ex.StackTrace,LogOnly := True)       
                    PrintMsg(My.Resources.ResText_IRFailed)
                End Try
                LockGUI(False)
            End Sub)
        LockGUI()
        th.Start()
    End Sub
    Private Sub 更新数据区索引ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 更新数据区索引ToolStripMenuItem.Click
        Dim th As New Threading.Thread(
        Sub()
            Try
                If (My.Settings.LTFSWriter_ForceIndex OrElse TotalBytesUnindexed <> 0) AndAlso schema IsNot Nothing AndAlso schema.location.partition = ltfsindex.PartitionLabel.b Then
                    WriteCurrentIndex(False)
                    TapeUtils.Flush(TapeDrive)
                    PrintMsg(My.Resources.ResText_DPIWritten)
                End If
            Catch ex As Exception
                PrintMsg(My.Resources.ResText_DPIWFailed, False, $"{My.Resources.ResText_DPIWFailed}: {ex.ToString}")
                Invoke(Sub() MessageBox.Show(ex.ToString()))
            End Try
            Invoke(Sub()
                       LockGUI(False)
                       RefreshDisplay()
                       If Not SilentMode Then MessageBox.Show(My.Resources.ResText_DPIUed)
                   End Sub)
        End Sub)
        LockGUI()
        th.Start()
    End Sub
    Public Sub LoadIndexFile(FileName As String, Optional ByVal Silent As Boolean = False)
        Try
            PrintMsg(My.Resources.ResText_RI)
            PrintMsg(My.Resources.ResText_AI)
            Dim sch2 As ltfsindex = ltfsindex.FromSchFile(FileName)
            PrintMsg(My.Resources.ResText_AISucc)
            If sch2 IsNot Nothing Then
                schema = sch2
            Else
                Throw New Exception
            End If
            While True
                Threading.Thread.Sleep(0)
                SyncLock UFReadCount
                    If UFReadCount > 0 Then Continue While
                    UnwrittenFiles.Clear()
                    CurrentFilesProcessed = 0
                    CurrentBytesProcessed = 0

                    Exit While
                End SyncLock
            End While
            ExtraPartitionCount = schema.location.partition
            RefreshDisplay()
            Modified = False
            Dim MAM080C As TapeUtils.MAMAttribute = TapeUtils.MAMAttribute.FromTapeDrive(TapeDrive, 8, 12, 0)
            Dim VCI As Byte() = {}
            If MAM080C IsNot Nothing Then
                VCI = MAM080C.RawData
            End If
            If Not Silent Then MessageBox.Show($"{My.Resources.ResText_ILdedP}{vbCrLf}{vbCrLf}{My.Resources.ResText_VCID}{vbCrLf}{TapeUtils.Byte2Hex(VCI, True)}")
        Catch ex As Exception
PrintMsg(ex.Message+vbCrLf+ex.StackTrace,LogOnly := True)
            MessageBox.Show($"{My.Resources.ResText_IAErrp}{ex.Message}")
        End Try
    End Sub
    Private Sub 加载外部索引ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 加载外部索引ToolStripMenuItem.Click
        If OpenFileDialog1.ShowDialog = DialogResult.OK Then
            LoadIndexFile(OpenFileDialog1.FileName)
        End If
    End Sub
    Public Function AutoDump() As String
        Dim FileName As String = Barcode
        If FileName = "" Then FileName = schema.volumeuuid.ToString()
        If Not IO.Directory.Exists($"schema\{Barcode}") Then
            IO.Directory.CreateDirectory($"schema\{Barcode}")
        End If
        If Not IO.Directory.Exists($"cm\{Barcode}") Then
            IO.Directory.CreateDirectory($"cm\{Barcode}")
        End If
        If Not IO.Directory.Exists($"schema最新") Then
            IO.Directory.CreateDirectory($"schema最新")
        End If
        Dim UsedSpace As String
        Dim cmData As New TapeUtils.CMParser(TapeDrive)
        Try

            Dim CMReport As String = cmData.GetReport(UsedSpace)
            Dim outputfileCM As String = $"cm\{Barcode}\LTFSIndex_Autosave_{FileName _
            }_GEN{schema.generationnumber _
            }_P{schema.location.partition _
            }_B{schema.location.startblock _
            }_{UsedSpace}_{Now.ToString("yyyyMMdd_HHmmss.fffffff")}.cm"
            If CMReport.Length > 0 Then IO.File.WriteAllText(outputfileCM, CMReport)
        Catch ex As Exception
                    PrintMsg(ex.Message+vbCrLf+ex.StackTrace,LogOnly := True)
        End Try
        Dim outputfile As String = $"schema\{Barcode}\LTFSIndex_Autosave_{FileName _
            }_GEN{schema.generationnumber _
            }_P{schema.location.partition _
            }_B{schema.location.startblock _
            }_{UsedSpace}_{Now.ToString("yyyyMMdd_HHmmss.fffffff")}.schema"


        If Not IO.Directory.Exists(IO.Path.Combine(Application.StartupPath, "schema")) Then
            IO.Directory.CreateDirectory(IO.Path.Combine(Application.StartupPath, "schema"))
        End If
        Dim outputfileBarCode As String = $"schema最新\{Barcode}.schema"
        outputfile = IO.Path.Combine(Application.StartupPath, outputfile)
        PrintMsg(My.Resources.ResText_Exporting)
        schema.SaveFile(outputfile)
        ‘复制文件
         IO.File.Copy(outputfile, outputfileBarCode, True)
'        schema.SaveFile(outputfileBarCode)


        PrintMsg(My.Resources.ResText_IndexBaked, False, $"{My.Resources.ResText_IndexBak2}{vbCrLf}{outputfile}")
        Return outputfile
    End Function
    Private Sub 备份当前索引ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 备份当前索引ToolStripMenuItem.Click
        Dim th As New Threading.Thread(
            Sub()
                Try
                    Dim outputfile As String = AutoDump()
                    Me.Invoke(Sub() MessageBox.Show($"{My.Resources.ResText_IndexBak2}{vbCrLf}{outputfile}"))
                Catch ex As Exception
                    PrintMsg(ex.Message+vbCrLf+ex.StackTrace,LogOnly := True)

                    PrintMsg(My.Resources.ResText_IndexBakF)
                End Try
                LockGUI(False)
            End Sub)
        LockGUI()
        th.Start()
    End Sub
    Private Sub 格式化ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 格式化ToolStripMenuItem.Click
        If MessageBox.Show(My.Resources.ResText_DataLossWarning, My.Resources.ResText_Warning, MessageBoxButtons.OKCancel) = DialogResult.OK Then
            While True
                Threading.Thread.Sleep(0)
                SyncLock UFReadCount
                    If UFReadCount > 0 Then Continue While
                    UnwrittenFiles.Clear()
                    CurrentFilesProcessed = 0
                    CurrentBytesProcessed = 0

                    Exit While
                End SyncLock
            End While
            'nop
            TapeUtils.ReadPosition(TapeDrive)
            Dim plabelCountfile = $"plabel\\{Barcode}" & ".count"
            IO.File.Delete(plabelCountfile)
            Dim MaxExtraPartitionAllowed As Byte = TapeUtils.ModeSense(TapeDrive, &H11)(2)
            If MaxExtraPartitionAllowed > 1 Then MaxExtraPartitionAllowed = 1
            '强制根据配置分区
            'If Not My.Settings.MaxExtraPartitionAllowed.Equals(Nothing) Then
            'MaxExtraPartitionAllowed = My.Settings.MaxExtraPartitionAllowed
            'End If
            Barcode = TapeUtils.ReadBarcode(TapeDrive)
            Dim VolumeLabel As String = ""
            Dim Confirm As Boolean = False
            While Not Confirm
                Barcode = InputBox(My.Resources.ResText_SetBarcode, My.Resources.ResText_Barcode, Barcode)
                If VolumeLabel = "" Then VolumeLabel = Barcode
                VolumeLabel = InputBox(My.Resources.ResText_SetVolumeN, My.Resources.ResText_LTFSVolumeN, VolumeLabel)

                Select Case MessageBox.Show($"{My.Resources.ResText_Barcode2}{Barcode}{vbCrLf}{My.Resources.ResText_LTFSVolumeN2}{VolumeLabel}", My.Resources.ResText_Confirm, MessageBoxButtons.YesNoCancel)
                    Case DialogResult.Yes
                        Confirm = True
                        Exit While
                    Case DialogResult.No
                        Confirm = False
                    Case DialogResult.Cancel
                        Exit Sub
                End Select
            End While
            LockGUI()
            Dim DefaultBlockSize As Long = 524288

            If MaxExtraPartitionAllowed = 0 Then DefaultBlockSize = 524288
            TapeUtils.mkltfs(TapeDrive, Barcode, VolumeLabel, MaxExtraPartitionAllowed, DefaultBlockSize, False,
                Sub(Message As String)
                    'ProgressReport
                    PrintMsg(Message)
                End Sub,
                Sub(Message As String)
                    'OnFinished
                    PrintMsg(My.Resources.ResText_FmtFin)
                    LockGUI(False)
                    Me.Invoke(Sub()
                                  MessageBox.Show(My.Resources.ResText_FmtFin)
                                  读取索引ToolStripMenuItem_Click(sender, e)
                              End Sub)
                End Sub,
                Sub(Message As String)
                    'OnError
                    PrintMsg(Message)
                    LockGUI(False)
                    Me.Invoke(Sub() MessageBox.Show($"{My.Resources.ResText_FmtFail}{vbCrLf}{Message}"))
                End Sub)
        End If
    End Sub
    Public Function ImportSHA1(schhash As ltfsindex, Overwrite As Boolean) As String
        Dim fprocessed As Integer = 0, fhash As Integer = 0
        Dim q As New List(Of IOManager.IndexedLHashDirectory)
        q.Add(New IOManager.IndexedLHashDirectory(schema._directory(0), schhash._directory(0)))
        While q.Count > 0
            Dim qtmp As New List(Of IOManager.IndexedLHashDirectory)
            For Each d As IOManager.IndexedLHashDirectory In q
                For Each f As ltfsindex.file In d.LTFSIndexDir.contents._file
                    Try
                        For Each flookup As ltfsindex.file In d.LHash_Dir.contents._file
                            If flookup.name = f.name And flookup.length = f.length Then
                                Dim sha1value0 As String = f.GetXAttr(ltfsindex.file.xattr.HashType.SHA1)
                                Dim md5value0 As String = f.GetXAttr(ltfsindex.file.xattr.HashType.MD5)
                                If Not Overwrite Then
                                    If Not (sha1value0 IsNot Nothing AndAlso sha1value0 <> "" AndAlso sha1value0.Length = 40) Then
                                        PrintMsg($"{f.name}", False, $"{f.name}    {sha1value0} -> { flookup.GetXAttr(ltfsindex.file.xattr.HashType.SHA1)}")
                                        f.SetXattr(ltfsindex.file.xattr.HashType.SHA1, flookup.GetXAttr(ltfsindex.file.xattr.HashType.SHA1))
                                    End If
                                    If Not (md5value0 IsNot Nothing AndAlso md5value0 <> "" AndAlso md5value0.Length = 32) Then
                                        PrintMsg($"{f.name}", False, $"{f.name}    {md5value0} -> { flookup.GetXAttr(ltfsindex.file.xattr.HashType.MD5)}")
                                        f.SetXattr(ltfsindex.file.xattr.HashType.MD5, flookup.GetXAttr(ltfsindex.file.xattr.HashType.MD5))
                                    End If
                                Else
                                    If flookup.GetXAttr(ltfsindex.file.xattr.HashType.SHA1) IsNot Nothing AndAlso flookup.GetXAttr(ltfsindex.file.xattr.HashType.SHA1) <> "" And flookup.GetXAttr(ltfsindex.file.xattr.HashType.SHA1).Length = 40 Then
                                        PrintMsg($"{f.name}", False, $"{f.name}    {sha1value0} -> { flookup.GetXAttr(ltfsindex.file.xattr.HashType.SHA1)}")
                                        f.SetXattr(ltfsindex.file.xattr.HashType.SHA1, flookup.GetXAttr(ltfsindex.file.xattr.HashType.SHA1))
                                    End If
                                    If flookup.GetXAttr(ltfsindex.file.xattr.HashType.MD5) IsNot Nothing AndAlso flookup.GetXAttr(ltfsindex.file.xattr.HashType.MD5) <> "" And flookup.GetXAttr(ltfsindex.file.xattr.HashType.MD5).Length = 32 Then
                                        PrintMsg($"{f.name}", False, $"{f.name}    {md5value0} -> { flookup.GetXAttr(ltfsindex.file.xattr.HashType.MD5)}")
                                        f.SetXattr(ltfsindex.file.xattr.HashType.MD5, flookup.GetXAttr(ltfsindex.file.xattr.HashType.MD5))
                                    End If
                                End If

                                Exit For
                            End If
                        Next
                        f.openforwrite = False
                        Threading.Interlocked.Increment(fprocessed)
                        If f.sha1.Length = 40 Then
                            Threading.Interlocked.Increment(fhash)
                        ElseIf fprocessed - fhash <= 5 Then
                            MessageBox.Show($"{f.fileuid}:{d.LTFSIndexDir.name}\{f.name} {f.sha1}")
                        End If
                    Catch ex As Exception
                                        PrintMsg(ex.Message+vbCrLf+ex.StackTrace,LogOnly := True)
                        PrintMsg(ex.ToString)
                    End Try
                Next
                For Each sd As ltfsindex.directory In d.LTFSIndexDir.contents._directory
                    For Each dlookup As ltfsindex.directory In d.LHash_Dir.contents._directory
                        If dlookup.name = sd.name Then
                            qtmp.Add(New IOManager.IndexedLHashDirectory(sd, dlookup))
                            Exit For
                        End If
                    Next
                Next
            Next
            q = qtmp
        End While
        If TotalBytesUnindexed = 0 Then TotalBytesUnindexed = 1
        Return ($"{fhash}/{fprocessed}")
    End Function
    Private Sub 合并SHA1ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 合并SHA1ToolStripMenuItem.Click
        If OpenFileDialog1.ShowDialog = DialogResult.OK Then
            Try
                Dim schhash As ltfsindex
                PrintMsg(My.Resources.ResText_RI)
                schhash = ltfsindex.FromSchFile(OpenFileDialog1.FileName)
                Dim dr As DialogResult = MessageBox.Show(My.Resources.ResText_SHA1Overw, My.Resources.ResText_Hint, MessageBoxButtons.YesNoCancel)
                PrintMsg(My.Resources.ResText_Importing)
                Dim result As String = ""
                If dr = DialogResult.Yes Then
                    result = ImportSHA1(schhash, True)
                ElseIf dr = DialogResult.No Then
                    result = ImportSHA1(schhash, False)
                Else
                    PrintMsg(My.Resources.ResText_OpCancelled)
                    Exit Try
                End If
                RefreshDisplay()
                PrintMsg($"{My.Resources.ResText_Imported} {result}")
            Catch ex As Exception
                                PrintMsg(ex.Message+vbCrLf+ex.StackTrace,LogOnly := True)
                PrintMsg(ex.ToString)
            End Try
        End If
    End Sub
    Private Sub 设置高度ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 设置高度ToolStripMenuItem.Click
        Dim p As TapeUtils.PositionData = GetPos
        PrintMsg($"Position = {p.ToString()}", LogOnly:=True)
        Dim Pos As Long = p.BlockNumber
        If MessageBox.Show($"{My.Resources.ResText_SetH1}{Pos}{My.Resources.ResText_SetH2}{vbCrLf}{My.Resources.ResText_SetH3}", My.Resources.ResText_Confirm, MessageBoxButtons.OKCancel) = DialogResult.OK Then
            CurrentHeight = Pos
        End If
    End Sub
    Private Sub 定位到起始块ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 定位到起始块ToolStripMenuItem.Click
        If ListView1.SelectedItems IsNot Nothing AndAlso
        ListView1.SelectedItems.Count > 0 AndAlso
        ListView1.SelectedItems(0).Tag IsNot Nothing AndAlso
            TypeOf (ListView1.SelectedItems(0).Tag) Is ltfsindex.file Then

            Dim f As ltfsindex.file = ListView1.SelectedItems(0).Tag
            If f.extentinfo IsNot Nothing AndAlso f.extentinfo.Count > 0 Then
                Dim ext As ltfsindex.file.extent = f.extentinfo(0)
                Dim th As New Threading.Thread(
                        Sub()
                            PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                            TapeUtils.Locate(TapeDrive, ext.startblock, GetPartitionNumber(ext.partition), TapeUtils.LocateDestType.Block)
                            PrintMsg($"Position = {GetPos.ToString()}", LogOnly:=True)
                            LockGUI(False)
                            Invoke(Sub() MessageBox.Show($"{My.Resources.ResText_Located}{ext.startblock}"))
                            PrintMsg($"{My.Resources.ResText_Located}{ext.startblock}")
                        End Sub)
                LockGUI()
                PrintMsg(My.Resources.ResText_Locating)
                th.Start()
            End If
        End If
    End Sub
    Private Sub S60ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles S60ToolStripMenuItem.Click
        SMaxNum = 60
        Chart1.Titles(0).Text = S60ToolStripMenuItem.Text
    End Sub
    Private Sub Min5ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles Min5ToolStripMenuItem.Click
        SMaxNum = 300
        Chart1.Titles(0).Text = Min5ToolStripMenuItem.Text
    End Sub
    Private Sub Min10ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles Min10ToolStripMenuItem.Click
        SMaxNum = 600
        Chart1.Titles(0).Text = Min10ToolStripMenuItem.Text
    End Sub
    Private Sub Min30ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles Min30ToolStripMenuItem.Click
        SMaxNum = 1800
        Chart1.Titles(0).Text = Min30ToolStripMenuItem.Text
    End Sub
    Private Sub H1ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles H1ToolStripMenuItem.Click
        SMaxNum = 3600
        Chart1.Titles(0).Text = H1ToolStripMenuItem.Text
    End Sub
    Private Sub H3ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles H3ToolStripMenuItem.Click
        SMaxNum = 3600 * 3
        Chart1.Titles(0).Text = H3ToolStripMenuItem.Text
    End Sub
    Private Sub H6ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles H6ToolStripMenuItem.Click
        SMaxNum = 3600 * 6
        Chart1.Titles(0).Text = H6ToolStripMenuItem.Text
    End Sub
    Public Sub CheckFlush()
        If Threading.Interlocked.Exchange(Flush, False) Then
            PrintMsg("Flush Triggered", LogOnly:=True)
            Dim Loc As TapeUtils.PositionData = GetPos
            If Loc.EOP Then PrintMsg(My.Resources.ResText_EWEOM, True)
            PrintMsg($"Position = {Loc.ToString()}", LogOnly:=True)
            TapeUtils.Locate(TapeDrive, Loc.BlockNumber, Loc.PartitionNumber, TapeUtils.LocateDestType.Block)
            RefreshCapacity()
        End If
    End Sub
    Public Sub CheckClean(Optional ByVal LockVolume As Boolean = False)
        If Threading.Interlocked.Exchange(Clean, False) Then
            If (Now - Clean_last).TotalSeconds < 300 Then Exit Sub
            PrintMsg("Clean Triggered", LogOnly:=True)
            Clean_last = Now
            Dim Loc As TapeUtils.PositionData = GetPos
            If Loc.EOP Then PrintMsg(My.Resources.ResText_EWEOM, True)
            PrintMsg($"Position = {Loc.ToString()}", LogOnly:=True)
            If Not Loc.EOP Then
                TapeUtils.AllowMediaRemoval(TapeDrive)
                TapeUtils.LoadEject(TapeDrive, TapeUtils.LoadOption.Unthread)
                TapeUtils.LoadEject(TapeDrive, TapeUtils.LoadOption.LoadThreaded)
                TapeUtils.Locate(TapeDrive, Loc.BlockNumber, Loc.PartitionNumber, TapeUtils.LocateDestType.Block)
                If LockVolume Then TapeUtils.PreventMediaRemoval(TapeDrive)
            End If
            RefreshCapacity()
        End If
    End Sub
    Private Sub LinearToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles LinearToolStripMenuItem.Click
        Chart1.ChartAreas(0).AxisY.IsLogarithmic = False
        LinearToolStripMenuItem.Checked = True
        LogarithmicToolStripMenuItem.Checked = False
    End Sub
    Private Sub LogrithmToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles LogarithmicToolStripMenuItem.Click
        Chart1.ChartAreas(0).AxisY.IsLogarithmic = True
        LinearToolStripMenuItem.Checked = False
        LogarithmicToolStripMenuItem.Checked = True
    End Sub
    Private Sub ToolStripDropDownButton1_Click(sender As Object, e As EventArgs) Handles ToolStripDropDownButton1.Click
        Pause = True
        If MessageBox.Show(My.Resources.ResText_CancelConfirm, My.Resources.ResText_Warning, MessageBoxButtons.OKCancel) = DialogResult.OK Then
            StopFlag = True
        End If
        Pause = False
    End Sub
    Private Sub ToolStripDropDownButton2_Click(sender As Object, e As EventArgs) Handles ToolStripDropDownButton2.Click
        Flush = True
    End Sub
    Private Sub ToolStripDropDownButton3_Click(sender As Object, e As EventArgs) Handles ToolStripDropDownButton3.Click
        Clean = True
    End Sub
    Private Sub ToolStripStatusLabel4_Click(sender As Object, e As EventArgs) Handles ToolStripStatusLabel4.Click
        If MessageBox.Show(My.Resources.ResText_ClearWC, My.Resources.ResText_Confirm, MessageBoxButtons.OKCancel) = DialogResult.OK Then
            TotalBytesProcessed = 0
            TotalFilesProcessed = 0
        End If
    End Sub
    Private Sub WA0ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles WA0ToolStripMenuItem.Click
        WA0ToolStripMenuItem.Checked = True
        WA1ToolStripMenuItem.Checked = False
        WA2ToolStripMenuItem.Checked = False
        WA3ToolStripMenuItem.Checked = False
    End Sub
    Private Sub WA1ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles WA1ToolStripMenuItem.Click
        WA0ToolStripMenuItem.Checked = False
        WA1ToolStripMenuItem.Checked = True
        WA2ToolStripMenuItem.Checked = False
        WA3ToolStripMenuItem.Checked = False
    End Sub
    Private Sub WA2ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles WA2ToolStripMenuItem.Click
        WA0ToolStripMenuItem.Checked = False
        WA1ToolStripMenuItem.Checked = False
        WA2ToolStripMenuItem.Checked = True
        WA3ToolStripMenuItem.Checked = False
    End Sub
    Public Function CalculateChecksum(FileIndex As ltfsindex.file) As Dictionary(Of String, String)
        Dim HT As New IOManager.CheckSumBlockwiseCalculator
        If FileIndex.length > 0 Then
            Dim CreateNew As Boolean = True
          

            If FileIndex.extentinfo.Count > 1 Then FileIndex.extentinfo.Sort(New Comparison(Of ltfsindex.file.extent)(Function(a As ltfsindex.file.extent, b As ltfsindex.file.extent) As Integer
                                                                                                                          Return a.fileoffset.CompareTo(b.fileoffset)
                                                                                                                      End Function))
            For Each fe As ltfsindex.file.extent In FileIndex.extentinfo

                If RestorePosition.BlockNumber <> fe.startblock OrElse RestorePosition.PartitionNumber <> Math.Min(ExtraPartitionCount, fe.partition) Then
                    PrintMsg($"RestorePosition.BlockNumber: {RestorePosition.BlockNumber} != fe.startblock: {fe.startblock} Or RestorePosition.PartitionNumber: {RestorePosition.PartitionNumber} != fe.partition: {fe.partition} ExtrapartitionCount: {ExtrapartitionCount} GetPartitionNumber(fe.partition){GetPartitionNumber(fe.partition)}",LogOnly:=True)
                    TapeUtils.Locate(TapeDrive, fe.startblock, GetPartitionNumber(fe.partition))
                    RestorePosition = New TapeUtils.PositionData(TapeDrive)
                End If
                Dim TotalBytesToRead As Long = fe.bytecount
                Dim blk As Byte() = TapeUtils.ReadBlock(TapeDrive, BlockSizeLimit:=Math.Min(plabel.blocksize, TotalBytesToRead))
                SyncLock RestorePosition
                    RestorePosition.BlockNumber += 1
                End SyncLock
                If fe.byteoffset > 0 Then blk = blk.Skip(fe.byteoffset).ToArray()
                TotalBytesToRead -= blk.Length
                HT.Propagate(blk)
                Threading.Interlocked.Add(CurrentBytesProcessed, blk.Length)
                Threading.Interlocked.Add(TotalBytesProcessed, blk.Length)
                While TotalBytesToRead > 0
                    Dim startBlockTimestamp = DateTime.Now
                    Metric.FuncFileOperationDuration(Sub() 
                    blk = TapeUtils.ReadBlock(TapeDrive, BlockSizeLimit:=Math.Min(plabel.blocksize, TotalBytesToRead))
                    End Sub,  {Barcode, "TapeUtils_ReadBlock", ""})
                    Metric.OperationCounter.WithLabels("hashfile_block_size_"+blk.Length.ToString()).Inc()
                    Metric.FuncFileOperationDuration(Sub() 
                    SyncLock RestorePosition
                        RestorePosition.BlockNumber += 1
                    End SyncLock
                    End Sub,  {Barcode, "SyncLock_RestorePosition", ""})
                    Dim blklen As Integer = blk.Length
                    If blklen = 0 Then Exit While
                    If blklen > TotalBytesToRead Then blklen = TotalBytesToRead
                    TotalBytesToRead -= blk.Length
                    Metric.FuncFileOperationDuration(Sub() 
                    HT.Propagate(blk, blklen)
                    End Sub,  {Barcode, " HT_Propagate", ""})
                    Threading.Interlocked.Add(CurrentBytesProcessed, blk.Length)
                    Threading.Interlocked.Add(TotalBytesProcessed, blk.Length)
                    If StopFlag Then Return Nothing
                    While Pause
                        Threading.Thread.Sleep(10)
                    End While
                    Dim duration As TimeSpan = DateTime.Now - startBlockTimestamp
                    Metric.FileOperationDurationHistogram.WithLabels(Barcode, "CheckSum_Read_Block", "").Observe(duration.TotalMilliseconds)
                    Metric.FileOperationDurationSummary.WithLabels(Barcode, "CheckSum_Read_Block", "").Observe(duration.TotalMilliseconds)
                End While
             
            Next
        End If
        HT.ProcessFinalBlock()
        Threading.Interlocked.Increment(CurrentFilesProcessed)
        Threading.Interlocked.Increment(TotalFilesProcessed)
        Dim result As New Dictionary(Of String, String)
        result.Add("SHA1", HT.SHA1Value)
        result.Add("MD5", HT.MD5Value)
        Return result
    End Function

    Private Sub 生成标签ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 生成标签ToolStripMenuItem.Click
        If My.Settings.LTFSWriter_FileLabel = "" Then
            设置标签ToolStripMenuItem_Click(sender, e)
            Exit Sub
        End If
        If ListView1.Tag IsNot Nothing Then
            Dim d As ltfsindex.directory = ListView1.Tag
            For Each dir As ltfsindex.directory In d.contents._directory
                If My.Settings.LTFSWriter_FileLabel = " " OrElse CInt(Val(dir.name)).ToString = dir.name Then
                    Dim fl As String = $".{My.Settings.LTFSWriter_FileLabel}"
                    If fl = ". " Then fl = ""
                    Dim fExist As Boolean = False
                    For Each f As ltfsindex.file In d.contents._file
                        If f.name = $"{dir.name}{fl}" Then
                            fExist = True
                            Exit For
                        End If
                    Next
                    If Not fExist Then
                        Dim emptyfile As String = IO.Path.Combine(Application.StartupPath, "empty.file")
                        IO.File.WriteAllBytes(emptyfile, {})
                        Dim fnew As New FileRecord(emptyfile, d)
                        fnew.File.name = $"{dir.name}{fl}"
                        While True
                            Threading.Thread.Sleep(0)
                            SyncLock UFReadCount
                                If UFReadCount > 0 Then Continue While
                                UnwrittenFiles.Add(fnew)
                                Exit While
                            End SyncLock
                        End While
                    End If
                End If
            Next
            PrintMsg(My.Resources.ResText_OpSucc)
            RefreshDisplay()
        End If
    End Sub

    Private Sub 设置标签ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 设置标签ToolStripMenuItem.Click
        My.Settings.LTFSWriter_FileLabel = InputBox(My.Resources.ResText_DLS, My.Resources.ResText_DLT, My.Settings.LTFSWriter_FileLabel)
        PrintMsg($"{My.Resources.ResText_DLFin} .{My.Settings.LTFSWriter_FileLabel}")
    End Sub

    Private Sub ToolStripButton1_Click(sender As Object, e As EventArgs) Handles ToolStripButton1.Click
        导入文件ToolStripMenuItem_Click(sender, e)
    End Sub

    Private Sub ToolStripButton2_Click(sender As Object, e As EventArgs) Handles ToolStripButton2.Click

        写入数据ToolStripMenuItem_Click(sender, e)
    End Sub

    Private Sub ToolStripButton4_Click(sender As Object, e As EventArgs) Handles ToolStripButton4.Click
        备份当前索引ToolStripMenuItem_Click(sender, e)
    End Sub

    Private Sub ToolStripButton3_Click(sender As Object, e As EventArgs) Handles ToolStripButton3.Click
        Dim th As New Threading.Thread(
                Sub()
                    Try
                        If (My.Settings.LTFSWriter_ForceIndex OrElse TotalBytesUnindexed <> 0) AndAlso schema IsNot Nothing AndAlso schema.location.partition = ltfsindex.PartitionLabel.b Then
                            PrintMsg(My.Resources.ResText_UDI)
                            WriteCurrentIndex(False)
                            TapeUtils.Flush(TapeDrive)
                        End If
                        AutoDump()
                        PrintMsg(My.Resources.ResText_UI)
                        RefreshIndexPartition()
                        TapeUtils.ReleaseUnit(TapeDrive)
                        TapeUtils.AllowMediaRemoval(TapeDrive)
                        PrintMsg(My.Resources.ResText_IUd)
                        If schema IsNot Nothing AndAlso schema.location.partition = ltfsindex.PartitionLabel.a Then 更新数据区索引ToolStripMenuItem.Enabled = False
                        TapeUtils.LoadEject(TapeDrive, TapeUtils.LoadOption.Eject)
                        PrintMsg(My.Resources.ResText_Ejd)
                        Invoke(Sub()
                                   LockGUI(False)
                                   RefreshDisplay()
                                   RaiseEvent TapeEjected()
                               End Sub)
                    Catch ex As Exception
                        PrintMsg(My.Resources.ResText_IUErr)
                        LockGUI(False)
                    End Try
                End Sub)
        LockGUI(True)
        th.Start()

    End Sub

    Private Sub ToolStripButton5_Click(sender As Object, e As EventArgs) Handles ToolStripButton5.Click
        合并SHA1ToolStripMenuItem_Click(sender, e)
    End Sub

    Private Sub 校验源文件ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 校验源文件ToolStripMenuItem.Click
        If FolderBrowserDialog1.ShowDialog = DialogResult.OK Then
            Dim hw As New HashTaskWindow With {.schema = schema, .BaseDirectory = FolderBrowserDialog1.SelectedPath, .TargetDirectory = "", .DisableSkipInfo = True}
            Dim p As String = ""
            If OpenFileDialog1.FileName <> "" Then p = New IO.FileInfo(OpenFileDialog1.FileName).DirectoryName
            hw.schPath = Barcode & ".schema"
            If IO.Directory.Exists(p) Then
                hw.schPath = IO.Path.Combine(p, hw.schPath)
            End If
            hw.CheckBox2.Visible = False
            hw.CheckBox3.Visible = False
            hw.Button3.Visible = False
            hw.Button4.Visible = False
            hw.ShowDialog()
            Dim q As New List(Of ltfsindex.directory)
            Dim hcount As Integer = 0, fcount As Integer = 0
            For Each d As ltfsindex.directory In schema._directory
                q.Add(d)
            Next
            For Each f As ltfsindex.file In schema._file
                Threading.Interlocked.Increment(fcount)
                If f.sha1 IsNot Nothing AndAlso f.sha1.Length = 40 Then Threading.Interlocked.Increment(hcount)
            Next
            While q.Count > 0
                Dim q1 As New List(Of ltfsindex.directory)
                For Each d As ltfsindex.directory In q
                    For Each f1 As ltfsindex.file In d.contents._file
                        Threading.Interlocked.Increment(fcount)
                        If f1.sha1 IsNot Nothing AndAlso f1.sha1.Length = 40 Then Threading.Interlocked.Increment(hcount)
                    Next
                    For Each d1 As ltfsindex.directory In d.contents._directory
                        q1.Add(d1)
                    Next
                Next
                q = q1
            End While
            PrintMsg($"{hcount}/{fcount}")
            If TotalBytesUnindexed = 0 Then TotalBytesUnindexed = 1
            RefreshDisplay()
        End If
    End Sub

    Private Sub ToolStripButton6_Click(sender As Object, e As EventArgs) Handles ToolStripButton6.Click
        校验源文件ToolStripMenuItem_Click(sender, e)
    End Sub

    Private Sub 限速不限制ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 限速不限制ToolStripMenuItem.Click
        Dim sin As String = InputBox(My.Resources.ResText_WLimS, My.Resources.ResText_Setting, SpeedLimit)
        If sin = "" Then Exit Sub
        SpeedLimit = Val(sin)
    End Sub

    Private Sub 重装带前清洁次数3ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 重装带前清洁次数3ToolStripMenuItem.Click
        CleanCycle = Val(InputBox(My.Resources.ResText_CLNCS, My.Resources.ResText_Setting, CleanCycle))
    End Sub
    Public Sub HashSelectedFiles(Overwrite As Boolean, ValidOnly As Boolean)
        Dim fc As Long = 0, ec As Long = 0
        If ListView1.SelectedItems IsNot Nothing AndAlso
                ListView1.SelectedItems.Count > 0 Then
            Dim BasePath As String = FolderBrowserDialog1.SelectedPath
            LockGUI()
            Dim flist As New List(Of ltfsindex.file)
            For Each SI As ListViewItem In ListView1.SelectedItems
                If TypeOf SI.Tag Is ltfsindex.file Then
                    flist.Add(SI.Tag)
                End If
            Next

            Dim th As New Threading.Thread(
                    Sub()
                        Try
                            PrintMsg(My.Resources.ResText_Hashing)
                            StopFlag = False
                            CurrentBytesProcessed = 0
                            CurrentFilesProcessed = 0
                            UnwrittenSizeOverrideValue = 0
                            UnwrittenCountOverwriteValue = flist.Count
                            For Each FI As ltfsindex.file In flist
                                UnwrittenSizeOverrideValue += FI.length
                            Next
                            RestorePosition = New TapeUtils.PositionData(TapeDrive)
                            For Each FileIndex As ltfsindex.file In flist
                                If ValidOnly Then
                                    If (FileIndex.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) = "" OrElse (Not FileIndex.SHA1ForeColor.Equals(Color.Black))) AndAlso
                                       (FileIndex.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) = "" OrElse (Not FileIndex.MD5ForeColor.Equals(Color.Black))) Then
                                        'Skip
                                        Threading.Interlocked.Add(CurrentBytesProcessed, FileIndex.length)
                                        Threading.Interlocked.Increment(CurrentFilesProcessed)

                                    Else
                                        Dim result As Dictionary(Of String, String) = CalculateChecksum(FileIndex)
                                        If result IsNot Nothing Then
                                            If FileIndex.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) = result.Item("SHA1") Then
                                                FileIndex.SHA1ForeColor = Color.DarkGreen
                                            ElseIf FileIndex.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) <> "" Then
                                                FileIndex.SHA1ForeColor = Color.Red
                                                Threading.Interlocked.Increment(ec)
                                                PrintMsg($"SHA1 Mismatch at fileuid={FileIndex.fileuid} filename={FileIndex.name} sha1logged={FileIndex.sha1} sha1calc={result.Item("SHA1")}", ForceLog:=True, Warning:=True)
                                            End If
                                            If FileIndex.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) = result.Item("MD5") Then
                                                FileIndex.MD5ForeColor = Color.DarkGreen
                                            ElseIf FileIndex.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) <> "" Then
                                                FileIndex.MD5ForeColor = Color.Red
                                                Threading.Interlocked.Increment(ec)
                                                PrintMsg($"MD5 Mismatch at fileuid={FileIndex.fileuid} filename={FileIndex.name} md5logged={FileIndex.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True)} md5calc={result.Item("MD5")}", ForceLog:=True, Warning:=True)
                                            End If
                                        End If
                                    End If
                                ElseIf Overwrite Then
                                    Dim result As Dictionary(Of String, String) = CalculateChecksum(FileIndex)
                                    If result IsNot Nothing Then
                                        If FileIndex.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) <> result.Item("SHA1") Then
                                            FileIndex.SetXattr(ltfsindex.file.xattr.HashType.SHA1, result.Item("SHA1"))
                                            FileIndex.SHA1ForeColor = Color.Blue
                                        Else
                                            FileIndex.SHA1ForeColor = Color.Green
                                        End If
                                        If FileIndex.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) <> result.Item("MD5") Then
                                            FileIndex.SetXattr(ltfsindex.file.xattr.HashType.MD5, result.Item("MD5"))
                                            FileIndex.MD5ForeColor = Color.Blue
                                        Else
                                            FileIndex.MD5ForeColor = Color.Green
                                        End If
                                        If TotalBytesUnindexed = 0 Then TotalBytesUnindexed = 1
                                    End If
                                Else
                                    If FileIndex.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) = "" OrElse FileIndex.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) = "" Then
                                        Dim result As Dictionary(Of String, String) = CalculateChecksum(FileIndex)
                                        If result IsNot Nothing Then
                                            If FileIndex.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) <> result.Item("SHA1") Then
                                                FileIndex.SetXattr(ltfsindex.file.xattr.HashType.SHA1, result.Item("SHA1"))
                                                FileIndex.SHA1ForeColor = Color.Blue
                                            Else
                                                FileIndex.SHA1ForeColor = Color.Green
                                            End If
                                            If FileIndex.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) <> result.Item("MD5") Then
                                                FileIndex.SetXattr(ltfsindex.file.xattr.HashType.MD5, result.Item("MD5"))
                                                FileIndex.MD5ForeColor = Color.Blue
                                            Else
                                                FileIndex.MD5ForeColor = Color.Green
                                            End If
                                            If TotalBytesUnindexed = 0 Then TotalBytesUnindexed = 1
                                        End If
                                    Else
                                        Threading.Interlocked.Add(CurrentBytesProcessed, FileIndex.length)
                                        Threading.Interlocked.Increment(CurrentFilesProcessed)

                                    End If
                                End If
                                Threading.Interlocked.Increment(fc)
                                If StopFlag Then Exit For
                            Next
                        Catch ex As Exception
                            PrintMsg(My.Resources.ResText_HErr)
                        End Try
                        UnwrittenSizeOverrideValue = 0
                        UnwrittenCountOverwriteValue = 0
                        StopFlag = False
                        LockGUI(False)
                        RefreshDisplay()
                        PrintMsg($"{My.Resources.ResText_HFin} {fc - ec}/{fc} | {ec} {My.Resources.ResText_Error}")
                    End Sub)
            th.Start()
        End If
    End Sub
    Private Sub 计算并更新ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 计算并更新ToolStripMenuItem.Click
        HashSelectedFiles(True, False)
    End Sub

    Private Sub 计算并跳过已有校验ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 计算并跳过已有校验ToolStripMenuItem.Click
        HashSelectedFiles(False, False)
    End Sub
    Private Sub 仅验证ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 仅验证ToolStripMenuItem.Click
        HashSelectedFiles(False, True)
    End Sub
    Public Sub HashSelectedDirWithSqlite(selectedDir As ltfsindex.directory)
        LockGUI(True)
        Dim th As New Threading.Thread(
            Sub()
                DirProvider.HashSelectedDirWithSqlite(Me, selectedDir)
               LockGUI(False)
            End Sub)
        th.Start()
    End Sub
    Public Sub HashSelectedDir(selectedDir As ltfsindex.directory, Overwrite As Boolean, ValidateOnly As Boolean)
        Dim th As New Threading.Thread(
            Sub()
                Dim fc As Long = 0, ec As Long = 0
                PrintMsg(My.Resources.ResText_Hashing)
                Try
                    StopFlag = False
                    Dim FileList As New List(Of FileRecord)
                    Dim IterDir As Action(Of ltfsindex.directory, String) =
                        Sub(tapeDir As ltfsindex.directory, outputDir As String)
                            For Each f As ltfsindex.file In tapeDir.contents._file
                                FileList.Add(New FileRecord With {.File = f, .SourcePath = outputDir & "\" & f.name})
                                'RestoreFile(IO.Path.Combine(outputDir.FullName, f.name), f)
                            Next
                            For Each d As ltfsindex.directory In tapeDir.contents._directory
                                Dim dirOutput As String = outputDir & "\" & d.name
                                IterDir(d, dirOutput)
                            Next
                        End Sub
                    PrintMsg(My.Resources.ResText_PrepFile)
                    Dim ODir As String = selectedDir.name
                    'If Not IO.Directory.Exists(ODir) Then IO.Directory.CreateDirectory(ODir)
                    IterDir(selectedDir, ODir)
                    FileList.Sort(New Comparison(Of FileRecord)(Function(a As FileRecord, b As FileRecord) As Integer
                                                                    If a.File.extentinfo.Count = 0 And b.File.extentinfo.Count <> 0 Then Return 0.CompareTo(1)
                                                                    If b.File.extentinfo.Count = 0 And a.File.extentinfo.Count <> 0 Then Return 1.CompareTo(0)
                                                                    If a.File.extentinfo.Count = 0 And b.File.extentinfo.Count = 0 Then Return 0.CompareTo(0)
                                                                    If a.File.extentinfo(0).partition = ltfsindex.PartitionLabel.a And b.File.extentinfo(0).partition = ltfsindex.PartitionLabel.b Then Return 0.CompareTo(1)
                                                                    If a.File.extentinfo(0).partition = ltfsindex.PartitionLabel.b And b.File.extentinfo(0).partition = ltfsindex.PartitionLabel.a Then Return 1.CompareTo(0)
                                                                    Return a.File.extentinfo(0).startblock.CompareTo(b.File.extentinfo(0).startblock)
                                                                End Function))
                    CurrentBytesProcessed = 0
                    CurrentFilesProcessed = 0
                    UnwrittenSizeOverrideValue = 0
                    UnwrittenCountOverwriteValue = FileList.Count
                    For Each FI As FileRecord In FileList
                        UnwrittenSizeOverrideValue += FI.File.length
                    Next
                    PrintMsg(My.Resources.ResText_Hashing)
                    Dim c As Integer = 0
                    RestorePosition = New TapeUtils.PositionData(TapeDrive)
                    For Each fr As FileRecord In FileList
                        c += 1
                        PrintMsg($"{My.Resources.ResText_Hashing} [{c}/{FileList.Count}] {fr.File.name} {My.Resources.ResText_Size}:{IOManager.FormatSize(fr.File.length)}", False, $"{My.Resources.ResText_Hashing} [{c}/{FileList.Count}] {fr.SourcePath} {My.Resources.ResText_Size}:{fr.File.length}")
                        If ValidateOnly Then
                            If (fr.File.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) = "" OrElse (Not fr.File.SHA1ForeColor.Equals(Color.Black))) AndAlso
                               (fr.File.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) = "" OrElse (Not fr.File.MD5ForeColor.Equals(Color.Black))) Then
                                'skip
                                Threading.Interlocked.Add(CurrentBytesProcessed, fr.File.length)
                                Threading.Interlocked.Increment(CurrentFilesProcessed)

                            Else
                                Dim result As Dictionary(Of String, String) = CalculateChecksum(fr.File)
                                If result IsNot Nothing Then
                                    If fr.File.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) = result.Item("SHA1") Then
                                        fr.File.SHA1ForeColor = Color.Green
                                    ElseIf fr.File.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) <> "" Then
                                        fr.File.SHA1ForeColor = Color.Red
                                        PrintMsg($"SHA1 Mismatch at fileuid={fr.File.fileuid} filename={fr.File.name} sha1logged={fr.File.sha1} sha1calc={result.Item("SHA1")} size=｛fr.File.length｝", ForceLog:=True, Warning:=True)
                                        Threading.Interlocked.Increment(ec)
                                    End If
                                    If fr.File.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) = result.Item("MD5") Then
                                        fr.File.MD5ForeColor = Color.Green
                                    ElseIf fr.File.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) <> "" Then
                                        fr.File.MD5ForeColor = Color.Red
                                        PrintMsg($"SHA1 Mismatch at fileuid={fr.File.fileuid} filename={fr.File.name} sha1logged={fr.File.sha1} sha1calc={result.Item("SHA1")} size=｛fr.File.length｝", ForceLog:=True, Warning:=True)
                                        Threading.Interlocked.Increment(ec)
                                    End If
                                End If
                            End If
                        ElseIf Overwrite Then
                            Dim result As Dictionary(Of String, String) = CalculateChecksum(fr.File)
                            If fr.File.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) <> result.Item("SHA1") Then
                                fr.File.SetXattr(ltfsindex.file.xattr.HashType.SHA1, result.Item("SHA1"))
                                fr.File.SHA1ForeColor = Color.Blue
                            Else
                                fr.File.SHA1ForeColor = Color.Green
                            End If
                            If fr.File.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) <> result.Item("MD5") Then
                                fr.File.SetXattr(ltfsindex.file.xattr.HashType.MD5, result.Item("MD5"))
                                fr.File.MD5ForeColor = Color.Blue
                            Else
                                fr.File.MD5ForeColor = Color.Green
                            End If
                            If TotalBytesUnindexed = 0 Then TotalBytesUnindexed = 1
                        Else
                            If fr.File.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) = "" OrElse fr.File.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) = "" Then
                                Dim result As Dictionary(Of String, String) = CalculateChecksum(fr.File)
                                If fr.File.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) <> result.Item("SHA1") Then
                                    fr.File.SetXattr(ltfsindex.file.xattr.HashType.SHA1, result.Item("SHA1"))
                                    fr.File.SHA1ForeColor = Color.Blue
                                Else
                                    fr.File.SHA1ForeColor = Color.Green
                                End If
                                If fr.File.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) <> result.Item("MD5") Then
                                    fr.File.SetXattr(ltfsindex.file.xattr.HashType.MD5, result.Item("MD5"))
                                    fr.File.MD5ForeColor = Color.Blue
                                Else
                                    fr.File.SHA1ForeColor = Color.Green
                                End If
                                If TotalBytesUnindexed = 0 Then TotalBytesUnindexed = 1
                            Else
                                Threading.Interlocked.Add(CurrentBytesProcessed, fr.File.length)
                                Threading.Interlocked.Increment(CurrentFilesProcessed)

                            End If
                        End If
                        Threading.Interlocked.Increment(fc)
                        If StopFlag Then
                            PrintMsg(My.Resources.ResText_OpCancelled)
                            Exit Try
                        End If
                    Next
                    PrintMsg($"{My.Resources.ResText_HFin} {fc - ec}/{fc} | {ec} {My.Resources.ResText_Error}")
                Catch ex As Exception
                    Invoke(Sub() MessageBox.Show(ex.ToString))
                    PrintMsg(My.Resources.ResText_HErr)
                End Try
                UnwrittenSizeOverrideValue = 0
                UnwrittenCountOverwriteValue = 0
                LockGUI(False)
                RefreshDisplay()
            End Sub)
        LockGUI()
        th.Start()
    End Sub
    Private Sub 计算并更新ToolStripMenuItem1_Click(sender As Object, e As EventArgs) Handles 计算并更新ToolStripMenuItem1.Click
        If TreeView1.SelectedNode IsNot Nothing Then
            Dim selectedDir As ltfsindex.directory = TreeView1.SelectedNode.Tag
            HashSelectedDir(selectedDir, True, False)
        End If
    End Sub

    Private Sub 跳过已有校验ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 跳过已有校验ToolStripMenuItem.Click
        If TreeView1.SelectedNode IsNot Nothing Then
            Dim selectedDir As ltfsindex.directory = TreeView1.SelectedNode.Tag
            HashSelectedDir(selectedDir, False, False)
        End If
    End Sub

    Private Sub 仅验证ToolStripMenuItem1_Click(sender As Object, e As EventArgs) Handles 仅验证ToolStripMenuItem1.Click
        If TreeView1.SelectedNode IsNot Nothing Then
            Dim selectedDir As ltfsindex.directory = TreeView1.SelectedNode.Tag
            HashSelectedDirWithSqlite(selectedDir)
        End If
    End Sub

    Private Sub 复制选中信息ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 复制选中信息ToolStripMenuItem.Click
        Dim result As New StringBuilder
        If ListView1.Tag IsNot Nothing AndAlso
        ListView1.SelectedItems IsNot Nothing AndAlso
        ListView1.SelectedItems.Count > 0 Then
            SyncLock ListView1.SelectedItems
                For Each ItemSelected As ListViewItem In ListView1.SelectedItems
                    If ItemSelected.Tag IsNot Nothing AndAlso TypeOf (ItemSelected.Tag) Is ltfsindex.file Then
                        Dim f As ltfsindex.file = ItemSelected.Tag
                        result.AppendLine(f.name)
                    End If
                Next
            End SyncLock
        End If
        Clipboard.SetText(result.ToString)
    End Sub

    Private Sub 统计ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 统计ToolStripMenuItem.Click
        If TreeView1.SelectedNode IsNot Nothing Then
            If TypeOf TreeView1.SelectedNode.Tag IsNot ltfsindex.directory Then Exit Sub
            Dim d As ltfsindex.directory = TreeView1.SelectedNode.Tag
            Dim fnum As Long = 0, fbytes As Long = 0
            Dim q As New List(Of ltfsindex.directory)
            q.Add(d)
            While q.Count > 0
                Dim q2 As New List(Of ltfsindex.directory)
                For Each qd As ltfsindex.directory In q
                    Threading.Interlocked.Add(fnum, qd.contents._file.Count)
                    For Each qf As ltfsindex.file In qd.contents._file
                        Threading.Interlocked.Add(fbytes, qf.length)
                    Next
                    q2.AddRange(qd.contents._directory)
                Next
                q = q2
            End While
            MessageBox.Show($"{d.name}{vbCrLf}{My.Resources.ResText_FCountP}{fnum}{vbCrLf}{My.Resources.ResText_FSizeP}{fbytes} {My.Resources.ResText_Byte} ({IOManager.FormatSize(fbytes)})")
        End If
    End Sub

    Private Sub 预读文件数5ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 预读文件数5ToolStripMenuItem.Click
        Dim s As String = InputBox(My.Resources.ResText_SPreR, My.Resources.ResText_Setting, My.Settings.LTFSWriter_PreLoadNum)
        If s = "" Then Exit Sub
        My.Settings.LTFSWriter_PreLoadNum = Val(s)
        预读文件数5ToolStripMenuItem.Text = $"{My.Resources.ResText_PFC}{My.Settings.LTFSWriter_PreLoadNum}"
    End Sub

    Private Sub 文件缓存32MiBToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 文件缓存32MiBToolStripMenuItem.Click
        Dim s As String = InputBox("设置文件缓存", My.Resources.ResText_Setting, My.Settings.LTFSWriter_PreLoadBytes)
        If s = "" Then Exit Sub
        My.Settings.LTFSWriter_PreLoadBytes = Val(s)
        If My.Settings.LTFSWriter_PreLoadBytes = 0 Then My.Settings.LTFSWriter_PreLoadBytes = 4096
        文件缓存32MiBToolStripMenuItem.Text = $"文件缓存：{IOManager.FormatSize(My.Settings.LTFSWriter_PreLoadBytes)}"
    End Sub

    Private Sub 文件详情ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 文件详情ToolStripMenuItem.Click
        Dim result As New StringBuilder
        If ListView1.Tag IsNot Nothing AndAlso
        ListView1.SelectedItems IsNot Nothing AndAlso
        ListView1.SelectedItems.Count > 0 Then
            SyncLock ListView1.SelectedItems
                For Each ItemSelected As ListViewItem In ListView1.SelectedItems
                    If ItemSelected.Tag IsNot Nothing AndAlso TypeOf (ItemSelected.Tag) Is ltfsindex.file Then
                        Dim f As ltfsindex.file = ItemSelected.Tag
                        result.AppendLine(f.GetSerializedText())
                    End If
                Next
            End SyncLock
        End If
        MessageBox.Show(result.ToString)
    End Sub

    Private Sub 禁用分区ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 禁用分区ToolStripMenuItem.Click
        DisablePartition = 禁用分区ToolStripMenuItem.Checked
    End Sub

    Private Sub 速度下限ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 速度下限ToolStripMenuItem.Click
        Dim s As String = InputBox(My.Resources.ResText_SSMin, My.Resources.ResText_Setting, My.Settings.LTFSWriter_AutoCleanDownLim)
        If s = "" Then Exit Sub
        My.Settings.LTFSWriter_AutoCleanDownLim = Val(s)
        My.Settings.Save()
        速度下限ToolStripMenuItem.Text = $"{My.Resources.ResText_SMin}{My.Settings.LTFSWriter_AutoCleanDownLim} MiB/s"
    End Sub

    Private Sub 速度上限ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 速度上限ToolStripMenuItem.Click
        Dim s As String = InputBox(My.Resources.ResText_SSMax, My.Resources.ResText_Setting, My.Settings.LTFSWriter_AutoCleanUpperLim)
        If s = "" Then Exit Sub
        My.Settings.LTFSWriter_AutoCleanUpperLim = Val(s)
        My.Settings.Save()
        速度上限ToolStripMenuItem.Text = $"{My.Resources.ResText_SMax}{My.Settings.LTFSWriter_AutoCleanUpperLim} MiB/s"
    End Sub

    Private Sub 持续时间ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 持续时间ToolStripMenuItem.Click
        Dim s As String = InputBox(My.Resources.ResText_SSTime, My.Resources.ResText_Setting, My.Settings.LTFSWriter_AutoCleanTimeThreashould)
        If s = "" Then Exit Sub
        My.Settings.LTFSWriter_AutoCleanTimeThreashould = Val(s)
        My.Settings.Save()
        持续时间ToolStripMenuItem.Text = $"{My.Resources.ResText_STime}{My.Settings.LTFSWriter_AutoCleanTimeThreashould}s"
    End Sub

    Private Sub 去重SHA1ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 去重SHA1ToolStripMenuItem.Click
        My.Settings.LTFSWriter_DeDupe = Not My.Settings.LTFSWriter_DeDupe
        去重SHA1ToolStripMenuItem.Checked = My.Settings.LTFSWriter_DeDupe
        My.Settings.Save()
    End Sub

    Public Class LTFSMountFuseSvc
        Inherits Fsp.Service
        Public LW As LTFSWriter
        Public _Host As Fsp.FileSystemHost
        Public TapeDrive As String = ""
        Public ReadOnly Property MountPath As String
            Get
                Return TapeDrive.Split({"\"}, StringSplitOptions.RemoveEmptyEntries).Last
            End Get
        End Property
        Public Sub New()
            MyBase.New("LTFSMountFuseServie")
        End Sub

        Protected Overrides Sub OnStart(Args As String())
            ' 创建ProxyGenerator实例
            Dim generator As New ProxyGenerator()
            ' 创建LoggingInterceptor实例
            Dim interceptor As New LoggingInterceptor()
            ' 使用ProxyGenerator创建动态代理对象
'            Dim proxyInstance = generator.CreateClassProxy(Of LTFSMountFSBase)(interceptor)
            Dim proxyInstance = generator.CreateClassProxy(Of LTFSMountFSSqliteBase)(interceptor)
            proxyInstance.TapeDrive=TapeDrive
            proxyInstance.LW = LW
            Dim Host As New Fsp.FileSystemHost(proxyInstance)
            Host.Prefix = $"\ltfs\{MountPath}"
            Host.FileSystemName = "LTFS"
            Dim Code As Integer = Host.Mount("L:", Nothing, True, 0)
            _Host = Host
            'MessageBox.Show($"Code {Code} Name={Host.FileSystemName} MP={Host.MountPoint} Pf={Host.Prefix}")
        End Sub
        Protected Overrides Sub OnStop()
            _Host.Unmount()
            _Host = Nothing
        End Sub
    End Class
    Private Sub 挂载盘符只读ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 挂载盘符只读ToolStripMenuItem.Click
        '挂载
        Dim DriveLoc As String = TapeDrive
        If DriveLoc = "" Then DriveLoc = "\\.\TAPE0"
        Dim MountPath As String = DriveLoc.Split({"\"}, StringSplitOptions.RemoveEmptyEntries).ToList.Last
        Static svc As New LTFSMountFuseSvc()
        svc.LW = Me
        svc.TapeDrive = DriveLoc

        Task.Run(
            Sub()
                svc.Run()
            End Sub)
        WinFspPositionData =Nothing '让下次winfsp能够从EOD开始写入
        MessageBox.Show($"Mounted as \\ltfs\{svc.MountPath}{vbCrLf}Press OK to unmount")

        '卸载
'        svc.Stop()
        MessageBox.Show($"Unmounted. Code={svc.ExitCode}")
    End Sub

    Private Sub 子目录列表ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 子目录列表ToolStripMenuItem.Click
        Dim result As New StringBuilder
        If ListView1.Tag IsNot Nothing AndAlso TypeOf (ListView1.Tag) Is ltfsindex.directory Then
            For Each Dir As ltfsindex.directory In CType(ListView1.Tag, ltfsindex.directory).contents._directory
                result.AppendLine(Dir.name)
            Next
        End If
        Clipboard.SetText(result.ToString)
    End Sub

    Private Sub 文件详情ToolStripMenuItem1_Click(sender As Object, e As EventArgs) Handles 文件详情ToolStripMenuItem1.Click
        Dim result As New StringBuilder
        If ListView1.Tag IsNot Nothing AndAlso
        ListView1.SelectedItems IsNot Nothing AndAlso
        ListView1.SelectedItems.Count > 0 Then
            SyncLock ListView1.SelectedItems
                For Each ItemSelected As ListViewItem In ListView1.SelectedItems
                    If ItemSelected.Tag IsNot Nothing AndAlso TypeOf (ItemSelected.Tag) Is ltfsindex.file Then
                        Dim f As ltfsindex.file = ItemSelected.Tag
                        result.AppendLine(f.GetSerializedText())
                    End If
                Next
            End SyncLock
        End If
        Clipboard.SetText(result.ToString)
    End Sub

    Private Sub XAttrToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles XAttrToolStripMenuItem.Click
        Dim result As New StringBuilder
        If ListView1.Tag IsNot Nothing AndAlso
        ListView1.SelectedItems IsNot Nothing AndAlso
        ListView1.SelectedItems.Count > 0 Then
            SyncLock ListView1.SelectedItems
                For Each ItemSelected As ListViewItem In ListView1.SelectedItems
                    If ItemSelected.Tag IsNot Nothing AndAlso TypeOf (ItemSelected.Tag) Is ltfsindex.file Then
                        Dim f As ltfsindex.file = ItemSelected.Tag
                        result.AppendLine(f.GetXAttrText())
                    End If
                Next
            End SyncLock
        End If
        Clipboard.SetText(result.ToString)
    End Sub

    Private Sub 启动FTP服务只读ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 启动FTP服务只读ToolStripMenuItem.Click
        Dim svc As New FTPService()
        AddHandler svc.LogPrint, Sub(s As String)
                                     PrintMsg($"FTPSVC> {s}")
                                 End Sub
        svc.port = Integer.Parse(InputBox("Port", "FTP Service", "8021"))
        svc.schema = schema
        svc.TapeDrive = TapeDrive
        svc.BlockSize = plabel.blocksize
        svc.ExtraPartitionCount = ExtraPartitionCount
        svc.StartService()
        MessageBox.Show($"Service running on port {svc.port}.")
        svc.StopService()
        MessageBox.Show("Service stopped.")
    End Sub

    Private Sub 右下角显示容量损失ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 右下角显示容量损失ToolStripMenuItem.Click
        My.Settings.LTFSWriter_ShowLoss = Not My.Settings.LTFSWriter_ShowLoss
        右下角显示容量损失ToolStripMenuItem.Checked = My.Settings.LTFSWriter_ShowLoss
        My.Settings.Save()
    End Sub

    Private Sub 压缩索引ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 压缩索引ToolStripMenuItem.Click
        If TreeView1.SelectedNode IsNot Nothing AndAlso TypeOf TreeView1.SelectedNode.Tag Is ltfsindex.directory Then
            LockGUI(True)
            Dim d As ltfsindex.directory = TreeView1.SelectedNode.Tag
            Dim p As ltfsindex.directory = TreeView1.SelectedNode.Parent.Tag
            Task.Run(Sub()
                         Dim tmpf As String = $"{Application.StartupPath}\LDS_{Now.ToString("yyyyMMdd_HHmmss.fffffff")}.tmp"
                         d.SaveFile(tmpf)
                         Dim ms As New IO.FileStream(tmpf, IO.FileMode.Open)
                         TapeUtils.ReserveUnit(TapeDrive)
                         TapeUtils.PreventMediaRemoval(TapeDrive)
                         If Not LocateToWritePosition() Then Exit Sub
                         Dim pos As New TapeUtils.PositionData(TapeDrive)
                         Dim fadd As New ltfsindex.file With {.name = d.name,
                                 .accesstime = d.accesstime,
                                 .backuptime = d.backuptime,
                                 .changetime = d.changetime,
                                 .creationtime = d.creationtime,
                                 .modifytime = d.modifytime,
                                 .extendedattributes = {New ltfsindex.file.xattr With {.key = "ltfscopygui.archive", .value = "True"}}.ToList(),
                                 .fileuid = schema.highestfileuid,
                                 .length = ms.Length,
                                 .extentinfo = {New ltfsindex.file.extent With {
                                 .bytecount = ms.Length,
                                 .startblock = pos.BlockNumber,
                                 .byteoffset = 0,
                                 .fileoffset = 0,
                                 .partition = pos.PartitionNumber}
                                 }.ToList()}
                         p.contents._file.Add(fadd)

                         Dim LastWriteTask As Task = Nothing
                         Dim ExitWhileFlag As Boolean = False
                         Dim wBufferPtr As IntPtr = Marshal.AllocHGlobal(plabel.blocksize)
                         Dim sh As New IOManager.CheckSumBlockwiseCalculator
                         While Not StopFlag
                             Dim buffer(plabel.blocksize - 1) As Byte
                             Dim BytesReaded As Integer = ms.Read(buffer, 0, plabel.blocksize)
                             sh.Propagate(buffer, BytesReaded)
                             If ExitWhileFlag Then Exit While
                             If BytesReaded > 0 Then
                                 CheckCount += 1
                                 If CheckCount >= CheckCycle Then CheckCount = 0
                                 If SpeedLimit > 0 AndAlso CheckCount = 0 Then
                                     Dim ts As Double = (Now - SpeedLimitLastTriggerTime).TotalSeconds
                                     While SpeedLimit > 0 AndAlso ts > 0 AndAlso ((plabel.blocksize * CheckCycle / 1048576) / ts) > SpeedLimit
                                         Threading.Thread.Sleep(0)
                                         ts = (Now - SpeedLimitLastTriggerTime).TotalSeconds
                                     End While
                                     SpeedLimitLastTriggerTime = Now
                                 End If
                                 Marshal.Copy(buffer, 0, wBufferPtr, BytesReaded)
                                 Dim succ As Boolean = False
                                 While Not succ
                                     Dim sense As Byte()
                                     Try
                                         sense = TapeUtils.Write(TapeDrive, wBufferPtr, BytesReaded, True)
                                         SyncLock pos
                                             pos.BlockNumber += 1
                                         End SyncLock
                                     Catch ex As Exception
                                         Select Case MessageBox.Show(My.Resources.ResText_WErrSCSI, My.Resources.ResText_Warning, MessageBoxButtons.AbortRetryIgnore)
                                             Case DialogResult.Abort
                                                 Throw ex
                                             Case DialogResult.Retry
                                                 succ = False
                                             Case DialogResult.Ignore
                                                 succ = True
                                                 Exit While
                                         End Select
                                         pos = New TapeUtils.PositionData(TapeDrive)
                                         Continue While
                                     End Try
                                     If (((sense(2) >> 6) And &H1) = 1) Then
                                         If ((sense(2) And &HF) = 13) Then
                                             PrintMsg(My.Resources.ResText_VOF)
                                             Invoke(Sub() MessageBox.Show(My.Resources.ResText_VOF))
                                             StopFlag = True
                                             ms.Close()
                                             Exit Sub
                                         Else
                                             PrintMsg(My.Resources.ResText_EWEOM, True)
                                             succ = True
                                             Exit While
                                         End If
                                     ElseIf sense(2) And &HF <> 0 Then
                                         Select Case MessageBox.Show($"{My.Resources.ResText_WErr}{vbCrLf}{TapeUtils.ParseSenseData(sense)}{vbCrLf}{vbCrLf}sense{vbCrLf}{TapeUtils.Byte2Hex(sense, True)}", My.Resources.ResText_Warning, MessageBoxButtons.AbortRetryIgnore)
                                             Case DialogResult.Abort
                                                 Throw New Exception(TapeUtils.ParseSenseData(sense))
                                             Case DialogResult.Retry
                                                 succ = False
                                             Case DialogResult.Ignore
                                                 succ = True
                                                 Exit While
                                         End Select
                                         pos = New TapeUtils.PositionData(TapeDrive)
                                     Else
                                         succ = True
                                         Exit While
                                     End If
                                 End While
                                 If Flush Then CheckFlush()
                                 If Clean Then CheckClean(True)
                                 TotalBytesProcessed += BytesReaded
                                 CurrentBytesProcessed += BytesReaded
                                 TotalBytesUnindexed += BytesReaded
                             Else
                                 ExitWhileFlag = True
                             End If
                         End While
                         sh.ProcessFinalBlock()
                         fadd.SetXattr(ltfsindex.file.xattr.HashType.SHA1, sh.SHA1Value)
                         fadd.SetXattr(ltfsindex.file.xattr.HashType.MD5, sh.MD5Value)
                         If LastWriteTask IsNot Nothing Then LastWriteTask.Wait()
                         schema.highestfileuid += 1
                         p.contents._directory.Remove(d)
                         ms.Close()
                         IO.File.Delete(tmpf)
                         TotalFilesProcessed += 1
                         CurrentFilesProcessed += 1

                         Marshal.FreeHGlobal(wBufferPtr)
                         If TotalBytesUnindexed = 0 Then TotalBytesUnindexed = 1
                         pos = GetPos
                         If pos.EOP Then PrintMsg(My.Resources.ResText_EWEOM, True)
                         PrintMsg($"Position = {p.ToString()}", LogOnly:=True)
                         CurrentHeight = pos.BlockNumber
                         Invoke(Sub() 更新数据区索引ToolStripMenuItem.Enabled = True)
                         TapeUtils.Flush(TapeDrive)
                         TapeUtils.ReleaseUnit(TapeDrive)
                         TapeUtils.AllowMediaRemoval(TapeDrive)
                         Invoke(Sub() TreeView1.SelectedNode = TreeView1.SelectedNode.Parent)
                         RefreshDisplay()
                         RefreshCapacity()
                         LockGUI(False)
                     End Sub)
        End If
    End Sub

    Private Sub 解压索引ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 解压索引ToolStripMenuItem.Click
        If TreeView1.SelectedNode IsNot Nothing AndAlso TypeOf TreeView1.SelectedNode.Tag Is ltfsindex.file Then
            Dim f As ltfsindex.file = TreeView1.SelectedNode.Tag
            Dim d As ltfsindex.directory = TreeView1.SelectedNode.Parent.Tag
            If f.GetXAttr("ltfscopygui.archive").ToLower = "true" Then
                LockGUI(True)
                Task.Run(Sub()
                             Try
                            If Not IO.Directory.Exists(IO.Path.Combine(Application.StartupPath, "LDS_cache")) Then
                                 IO.Directory.CreateDirectory(IO.Path.Combine(Application.StartupPath, "LDS_cache"))
                             End If
                             Dim tmpf As String = $"{Application.StartupPath}\LDS_cache\{f.sha1}.index.lds"
                             If Not IO.File.Exists(tmpf) Then
                                 RestorePosition = New TapeUtils.PositionData(TapeDrive)
                                 RestoreFile(tmpf, f)
                             end if
                                 Dim dindex As ltfsindex.directory = ltfsindex.directory.FromFile(tmpf)
                                 d.contents._file.Remove(f)
                                 d.contents._directory.Add(dindex)
                            if False Then
                                 IO.File.Delete(tmpf)
                            End If
                             Catch ex As Exception
                                 PrintMsg($"解压索引出错：{ex.ToString}", ForceLog:=True)
                             End Try

                             RefreshDisplay()
                             LockGUI(False)
                         End Sub)

            End If
        End If
    End Sub

    Private Sub 跳过符号链接ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 跳过符号链接ToolStripMenuItem.Click
        My.Settings.LTFSWriter_SkipSymlink = 跳过符号链接ToolStripMenuItem.Checked
    End Sub

    Private Sub 覆盖已有文件ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 覆盖已有文件ToolStripMenuItem.Click
        My.Settings.LTFSWriter_OverwriteExist = 覆盖已有文件ToolStripMenuItem.Checked
    End Sub

    Private Sub 显示文件数ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 显示文件数ToolStripMenuItem.Click
        My.Settings.LTFSWriter_ShowFileCount = 显示文件数ToolStripMenuItem.Checked
        RefreshDisplay()
    End Sub

    Private Sub 移动到索引区ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 移动到索引区ToolStripMenuItem.Click
        If ListView1.SelectedItems IsNot Nothing AndAlso
        ListView1.SelectedItems.Count > 0 Then
            LockGUI()
            Dim flist As New List(Of ltfsindex.file)
            For Each SI As ListViewItem In ListView1.SelectedItems
                If TypeOf SI.Tag Is ltfsindex.file Then
                    flist.Add(SI.Tag)
                End If
            Next
            Dim th As New Threading.Thread(
                    Sub()
                        Try
                            CurrentFilesProcessed = 0
                            CurrentBytesProcessed = 0
                            UnwrittenSizeOverrideValue = 0
                            UnwrittenCountOverwriteValue = flist.Count
                            For Each FI As ltfsindex.file In flist
                                UnwrittenSizeOverrideValue += FI.length
                            Next
                            PrintMsg(My.Resources.ResText_Writing)
                            StopFlag = False
                            TapeUtils.ReserveUnit(TapeDrive)
                            TapeUtils.PreventMediaRemoval(TapeDrive)
                            RestorePosition = New TapeUtils.PositionData(TapeDrive)
                            For Each FileIndex As ltfsindex.file In flist
                                MoveToIndexPartition(FileIndex)
                                If StopFlag Then
                                    PrintMsg(My.Resources.ResText_OpCancelled)
                                    Exit Sub
                                End If
                            Next
                        Catch ex As Exception
                            PrintMsg(My.Resources.ResText_RestoreErr)
                        End Try
                        TapeUtils.AllowMediaRemoval(TapeDrive)
                        TapeUtils.ReleaseUnit(TapeDrive)
                        StopFlag = False
                        UnwrittenSizeOverrideValue = 0
                        UnwrittenCountOverwriteValue = 0
                        LockGUI(False)
                        PrintMsg(My.Resources.ResText_AddFin)
                        Invoke(Sub()
                                   RefreshDisplay()
                                   MessageBox.Show(My.Resources.ResText_AddFin)
                               End Sub)
                    End Sub)
            th.Start()
        End If
    End Sub

    Private Sub 索引间隔36GiBToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 索引间隔36GiBToolStripMenuItem.Click
        IndexWriteInterval = Val(InputBox(My.Resources.ResText_SIIntv, My.Resources.ResText_Setting, IndexWriteInterval))
    End Sub
    Private Sub TreeMode_Click(sender As Object, e As EventArgs) Handles TreeMode.Click
        IsSqliteTreeView = Not IsSqliteTreeView
        RefreshDisplay()
    End Sub
    Private Sub 查找指定位置前的索引ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 查找指定位置前的索引ToolStripMenuItem.Click
        Dim blocknum As Long = CLng(InputBox("Block number", "Index search", "0"))
        If blocknum <= 0 Then Exit Sub
        Dim th As New Threading.Thread(
            Sub()
                Try
                    PrintMsg(My.Resources.ResText_Locating)
                    Dim data As Byte()
                    Dim currentPos As TapeUtils.PositionData = GetPos
                    PrintMsg($"Position = {currentPos.ToString()}", LogOnly:=True)
                    If ExtraPartitionCount = 0 Then
                        TapeUtils.Locate(TapeDrive, blocknum, 0)
                    Else
                        If currentPos.PartitionNumber <> 1 Then TapeUtils.Locate(TapeDrive, 0, 1, TapeUtils.LocateDestType.Block)
                        TapeUtils.Locate(TapeDrive, blocknum, DataPartition)
                    End If
                    PrintMsg(My.Resources.ResText_RI)
                    currentPos = GetPos
                    PrintMsg($"Position = {currentPos.ToString()}", LogOnly:=True)
                    If DisablePartition Then
                        TapeUtils.Space6(TapeDrive, -2, TapeUtils.LocateDestType.FileMark)
                    Else
                        Dim FM As Long = currentPos.FileNumber
                        If FM <= 1 Then
                            PrintMsg(My.Resources.ResText_IRFailed)
                            Invoke(Sub() MessageBox.Show(My.Resources.ResText_NLTFS, My.Resources.ResText_Error))
                            LockGUI(False)
                            Exit Try
                        End If
                        TapeUtils.Locate(TapeDrive, FM - 1, DataPartition, TapeUtils.LocateDestType.FileMark)
                    End If

                    TapeUtils.ReadFileMark(TapeDrive)
                    PrintMsg(My.Resources.ResText_RI)
                    Dim outputfile As String = "schema\LTFSIndex_LoadDPIndex_" & Now.ToString("yyyyMMdd_HHmmss.fffffff") & ".schema"
                    If Not IO.Directory.Exists(IO.Path.Combine(Application.StartupPath, "schema")) Then
                        IO.Directory.CreateDirectory(IO.Path.Combine(Application.StartupPath, "schema"))
                    End If
                    outputfile = IO.Path.Combine(Application.StartupPath, outputfile)
                    TapeUtils.ReadToFileMark(TapeDrive, outputfile)
                    PrintMsg(My.Resources.ResText_AI)
                    schema = ltfsindex.FromSchFile(outputfile)
                    PrintMsg(My.Resources.ResText_AISucc)
                    While True
                        Threading.Thread.Sleep(0)
                        SyncLock UFReadCount
                            If UFReadCount > 0 Then Continue While
                            UnwrittenFiles.Clear()
                            CurrentFilesProcessed = 0
                            CurrentBytesProcessed = 0
                            TotalBytesUnindexed = 0
                            Exit While
                        End SyncLock
                    End While
                    Modified = False
                    Me.Invoke(Sub()
                                  ToolStripStatusLabel1.ToolTipText = ToolStripStatusLabel1.Text
                                  MaxCapacity = 0
                                  RefreshDisplay()
                                  RefreshCapacity()
                              End Sub)
                    CurrentHeight = -1
                    PrintMsg(My.Resources.ResText_IRSucc)
                Catch ex As Exception
                    PrintMsg(My.Resources.ResText_IRFailed)
                End Try
                LockGUI(False)
            End Sub)
        LockGUI()
        th.Start()
    End Sub



    Private Sub 容量刷新间隔30sToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles 容量刷新间隔30sToolStripMenuItem.Click
        Dim s As String = InputBox(My.Resources.ResText_SCIntv, My.Resources.ResText_Setting, CapacityRefreshInterval)
        If s = "" Then Exit Sub
        CapacityRefreshInterval = Val(s)
    End Sub


    Private Sub WA3ToolStripMenuItem_Click(sender As Object, e As EventArgs) Handles WA3ToolStripMenuItem.Click
        WA0ToolStripMenuItem.Checked = False
        WA1ToolStripMenuItem.Checked = False
        WA2ToolStripMenuItem.Checked = False
        WA3ToolStripMenuItem.Checked = True
    End Sub

    Private Sub LTFSWriter_KeyDown(sender As Object, e As KeyEventArgs) Handles Me.KeyDown
        Select Case e.KeyCode
            Case Keys.KeyCode.F5
                RefreshDisplay()
        End Select
    End Sub
End Class

Public NotInheritable Class FileDropHandler
    Implements IMessageFilter, IDisposable
    <DllImport("user32.dll", SetLastError:=True, CallingConvention:=CallingConvention.Winapi)>
    Private Shared Function ChangeWindowMessageFilterEx(ByVal hWnd As IntPtr, ByVal message As UInteger, ByVal action As ChangeFilterAction, pChangeFilterStruct As ChangeFilterStruct) As <MarshalAs(UnmanagedType.Bool)> Boolean

    End Function

    <DllImport("shell32.dll", SetLastError:=False, CallingConvention:=CallingConvention.Winapi)>
    Private Shared Sub DragAcceptFiles(ByVal hWnd As IntPtr, ByVal fAccept As Boolean)
    End Sub

    <DllImport("shell32.dll", SetLastError:=False, CharSet:=CharSet.Unicode, CallingConvention:=CallingConvention.Winapi)>
    Private Shared Function DragQueryFile(ByVal hWnd As IntPtr, ByVal iFile As UInteger, ByVal lpszFile As StringBuilder, ByVal cch As Integer) As UInteger

    End Function

    <DllImport("shell32.dll", SetLastError:=False, CallingConvention:=CallingConvention.Winapi)>
    Private Shared Sub DragFinish(ByVal hDrop As IntPtr)

    End Sub

    <StructLayout(LayoutKind.Sequential)>
    Private Structure ChangeFilterStruct

        Public CbSize As UInteger

        Public ExtStatus As ChangeFilterStatus
    End Structure

    Private Enum ChangeFilterAction As UInteger

        MSGFLT_RESET

        MSGFLT_ALLOW

        MSGFLT_DISALLOW
    End Enum

    Private Enum ChangeFilterStatus As UInteger

        MSGFLTINFO_NONE

        MSGFLTINFO_ALREADYALLOWED_FORWND

        MSGFLTINFO_ALREADYDISALLOWED_FORWND

        MSGFLTINFO_ALLOWED_HIGHER
    End Enum

    Private Const WM_COPYGLOBALDATA As UInteger = 73

    Private Const WM_COPYDATA As UInteger = 74

    Private Const WM_DROPFILES As UInteger = 563

    Private Const GetIndexCount As UInteger = 4294967295

    Private _ContainerControl As Control

    Private _DisposeControl As Boolean

    Public ReadOnly Property ContainerControl As Control
        Get
            Return _ContainerControl
        End Get
    End Property

    Public Sub New(ByVal containerControl As Control)
        Me.New(containerControl, False)

    End Sub

    Public Sub New(ByVal containerControl As Control, ByVal releaseControl As Boolean)
        Try
            _ContainerControl = containerControl
        Catch ex As Exception
            Throw New ArgumentNullException("control", "control is null.")
        End Try
        If containerControl.IsDisposed Then
            Throw New ObjectDisposedException("control")
        End If

        Me._DisposeControl = releaseControl
        Dim status = New ChangeFilterStruct With {.CbSize = 8}
        If Not ChangeWindowMessageFilterEx(containerControl.Handle, WM_DROPFILES, ChangeFilterAction.MSGFLT_ALLOW, Nothing) Then
            Throw New Win32Exception(Marshal.GetLastWin32Error)
        End If

        If Not ChangeWindowMessageFilterEx(containerControl.Handle, WM_COPYGLOBALDATA, ChangeFilterAction.MSGFLT_ALLOW, Nothing) Then
            Throw New Win32Exception(Marshal.GetLastWin32Error)
        End If

        If Not ChangeWindowMessageFilterEx(containerControl.Handle, WM_COPYDATA, ChangeFilterAction.MSGFLT_ALLOW, Nothing) Then
            Throw New Win32Exception(Marshal.GetLastWin32Error)
        End If

        DragAcceptFiles(containerControl.Handle, True)
        Application.AddMessageFilter(Me)
    End Sub

    Public Function PreFilterMessage(ByRef m As Message) As Boolean Implements IMessageFilter.PreFilterMessage
        If ((Me._ContainerControl Is Nothing) OrElse Me._ContainerControl.IsDisposed) Then
            Return False
        End If

        If Me._ContainerControl.AllowDrop Then
            _ContainerControl.AllowDrop = False
            Return False
        End If
        If (m.Msg = WM_DROPFILES) Then
            Dim handle = m.WParam
            Dim fileCount = DragQueryFile(handle, GetIndexCount, Nothing, 0)
            Dim fileNames((fileCount) - 1) As String
            Dim sb = New StringBuilder(262)
            Dim charLength = sb.Capacity
            Dim i As UInteger = 0
            Do While (i <fileCount)
                If (DragQueryFile(handle, i, sb, charLength) > 0) Then
                    fileNames(i) = sb.ToString
                End If

                i = (i + 1)
            Loop

            DragFinish(handle)
            Me._ContainerControl.AllowDrop = True
            Me._ContainerControl.DoDragDrop(fileNames, DragDropEffects.All)
            Me._ContainerControl.AllowDrop = False
            Return True
        End If

        Return False
    End Function

    Public Sub Dispose() Implements IDisposable.Dispose
        If (Me._ContainerControl Is Nothing) Then
            If (Me._DisposeControl AndAlso Not Me._ContainerControl.IsDisposed) Then
                Me._ContainerControl.Dispose()
            End If

            Application.RemoveMessageFilter(Me)
            Me._ContainerControl = Nothing
        End If

    End Sub
End Class
