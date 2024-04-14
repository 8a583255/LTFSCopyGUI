Imports System.Collections.ObjectModel
Imports System.IO
Imports Microsoft.VisualBasic.ApplicationServices
Imports Prometheus
Imports CsvHelper
Imports System.Globalization
Imports SchwabenCode.QuickIO
Imports System.Windows.Forms

Imports System.Runtime.InteropServices
Imports Microsoft.Win32.SafeHandles

Public Class WinApiFileHelper
    Public Const FILE_ATTRIBUTE_NORMAL As Short = &H80
    Public Const INVALID_HANDLE_VALUE As Short = -1
    Public Const GENERIC_READ As UInteger = &H80000000UI
    Public Const GENERIC_WRITE As UInteger = &H40000000UI
    Public Const CREATE_NEW As UInteger = 1
    Public Const CREATE_ALWAYS As UInteger = 2
    Public Const OPEN_EXISTING As UInteger = 3
    Public Const FILE_FLAG_NO_BUFFERING As UInteger = &H20000000UI  
    Public Const FILE_FLAG_SEQUENTIAL_SCAN As UInteger = &H08000000UI
    Public Const FILE_FLAG_WRITE_THROUGH As UInteger = &H80000000UI
    Public Const FILE_SHARE_READ As UInteger = &H1
    Public Const FILE_SHARE_WRITE As UInteger = &H2

    <DllImport("kernel32.dll", SetLastError:=True)>
    public Shared Function CreateFile(lpFileName As String, dwDesiredAccess As UInteger,
                                       dwShareMode As UInteger, lpSecurityAttributes As IntPtr,
                                       dwCreationDisposition As UInteger,
                                       dwFlagsAndAttributes As UInteger,
                                       hTemplateFile As IntPtr) As SafeFileHandle
    End Function
End Class
Namespace My
    ' 以下事件可用于 MyApplication: 
    ' Startup:应用程序启动时在创建启动窗体之前引发。
    ' Shutdown:在关闭所有应用程序窗体后引发。如果应用程序非正常终止，则不会引发此事件。
    ' UnhandledException:在应用程序遇到未经处理的异常时引发。
    ' StartupNextInstance:在启动单实例应用程序且应用程序已处于活动状态时引发。 
    ' NetworkAvailabilityChanged:在连接或断开网络连接时引发。
    Class MyApplication

        <System.Runtime.InteropServices.DllImport("kernel32.dll")>
        Public Shared Function AllocConsole() As Boolean

        End Function
        <System.Runtime.InteropServices.DllImport("kernel32.dll")>
        Shared Function FreeConsole() As Boolean

        End Function
        <System.Runtime.InteropServices.DllImport("kernel32.dll")>
        Shared Function AttachConsole(pid As Integer) As Boolean

        End Function
        Public Sub InitConsole()
            If Not AttachConsole(-1) Then
                AllocConsole()
            Else
                Dim CurrentLine As Integer = Console.CursorTop
                Console.SetCursorPosition(0, Console.CursorTop)
                Console.Write("".PadRight(Console.WindowWidth))
                Console.SetCursorPosition(0, CurrentLine - 1)
            End If
        End Sub
        Public Sub CloseConsole()
            System.Windows.Forms.SendKeys.SendWait("{ENTER}")
            FreeConsole()
        End Sub
        Public Sub CheckUAC(e As StartupEventArgs)
            If Not New Security.Principal.WindowsPrincipal(Security.Principal.WindowsIdentity.GetCurrent()).IsInRole(Security.Principal.WindowsBuiltInRole.Administrator) Then
                Process.Start(New ProcessStartInfo With {.FileName = Windows.Forms.Application.ExecutablePath, .Verb = "runas", .Arguments = String.Join(" ", e.CommandLine)})
                End
            End If
        End Sub
        '读取文件速度测试
        Private Sub ReadFileSpeed(filePath As String)

            Dim BufferSize As Integer = 16777216
            Dim fs = New IO.FileStream(filePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read, BufferSize, True)
            Dim bytesRead As Integer

            Dim PreReadBuffer(BufferSize - 1) As Byte
            Do
                bytesRead = fs.Read(PreReadBuffer, 0, BufferSize)
                Metric.OperationCounter.WithLabels(("read_file")).Inc(bytesRead)
                ' 处理读取到的数据
                If bytesRead > 0 Then
                    ' 在这里可以处理读取到的数据
                End If
            Loop While bytesRead > 0

            ' 关闭文件流
            fs.Close()
        End Sub
        Private Shared Function ReadEfu(efuFilePath As String) As DataTable
            Dim dt As New DataTable()

            Using textReader As New StreamReader(efuFilePath)
                Using csv As New CsvReader(textReader, CultureInfo.InvariantCulture)
                    csv.Read()
                    csv.ReadHeader()
                    For Each header In csv.HeaderRecord
                        dt.Columns.Add(header)
                    Next
                    While csv.Read()
                        Dim row = dt.NewRow()
                        For Each column As DataColumn In dt.Columns
                            row(column.ColumnName) = csv.GetField(column.DataType, column.ColumnName)
                        Next
                        dt.Rows.Add(row)
                    End While
                End Using
            End Using
            Return dt
        End Function
        '挂载磁带
        Private Sub MountTape(tapedrive As String)
            Dim DriveLoc As String = tapedrive
            If DriveLoc = "" Then DriveLoc = "\\.\TAPE0"
            Dim MountPath As String = DriveLoc.Split({"\"}, StringSplitOptions.RemoveEmptyEntries).ToList.Last
            Static svc As New LTFSWriter.LTFSMountFuseSvc()
            svc.TapeDrive = DriveLoc

            Task.Run(
                Sub()
                    svc.Run()
                End Sub)

            MessageBox.Show($"Mounted as \\ltfs\{svc.MountPath}{vbCrLf}Press OK to unmount")

            '卸载
            svc.Stop()
        End Sub
        '读取文件速度测试
        Private Sub WriteTapeFileWithEfuSpeed(efuFilePath As String, tapedrive As String)
            Dim sense As Byte()
            Dim BufferSize As Integer = 4096
            Dim PreReadBuffer(BufferSize - 1) As Byte
            Dim bytesRead As Integer
            '            TapeUtils.Locate(tapedrive, 0, 1, TapeUtils.LocateDestType.EOD)
            Dim dt = ReadEfu(efuFilePath)
            For i As Integer = 0 To dt.Rows.Count - 1
                Metric.OperationCounter.WithLabels(("file_count")).Inc()
                Dim filePath = dt.Rows(i)("Filename")
                Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} open file " & i & " " & filePath)
                '                Dim fs = New IO.FileStream(filePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read, BufferSize, False)
'                Dim fs = QuickIOFile.OpenRead("\\?\" + filePath)
'                
                Dim handle As SafeFileHandle = WinApiFileHelper.CreateFile(
                    filePath,
                    WinApiFileHelper.GENERIC_READ,
                    0,
                    IntPtr.Zero,
                    WinApiFileHelper.OPEN_EXISTING,
                    WinApiFileHelper.FILE_ATTRIBUTE_NORMAL Or WinApiFileHelper.FILE_FLAG_NO_BUFFERING Or WinApiFileHelper.FILE_FLAG_SEQUENTIAL_SCAN,
                    IntPtr.Zero
                    )

                 
'                Do
'                    Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} read file " & i & " " & filePath)
'                    bytesRead = fs.Read(PreReadBuffer, 0, BufferSize)
'                    Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} readed file " & i & " " & filePath)
'                    Metric.OperationCounter.WithLabels(("read_file")).Inc(bytesRead)
'                    Exit Do
'                    ' 处理读取到的数据
'                    If bytesRead > 0 Then
'                        Dim succ As Boolean
'                        Dim offset As Integer = 0 ' 数据偏移量
'                        Dim plabelblocksize = 524288
'                        Dim buffer(plabelblocksize - 1) As Byte
'                        While offset < bytesRead ' 只处理实际读取的字节数
'
'                            ' 将 PreReadBuffer 中的数据复制到 buffer 中
'                            Array.Copy(PreReadBuffer, offset, buffer, 0, Math.Min(plabelblocksize, bytesRead - offset))
'
'                            sense = TapeUtils.Write(tapedrive, buffer, Math.Min(plabelblocksize, bytesRead - offset))
'                            Metric.OperationCounter.WithLabels(("write_file")).Inc(plabelblocksize)
'                            If ((sense(2) >> 6) And &H1) = 1 Then
'                                If (sense(2) And &HF) = 13 Then
'                                    '磁带已满                                           
'                                    Console.WriteLine("磁带已满")
'                                Else
'                                    '磁带即将写满
'                                    Console.WriteLine("磁带即将写满")
'                                End If
'                            ElseIf sense(2) And &HF <> 0 Then
'                                '写入出错
'                                Console.WriteLine("写入出错")
'                            Else
'                                succ = True
'                            End If
'                            offset += plabelblocksize ' 更新偏移量
'                        End While
'                    End If
'
'                Loop While bytesRead > 0
'
'                ' 关闭文件流
'                fs.Close()
'                Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} close file " & i & " " & filePath)
'
'
            Next



        End Sub
        '读取文件速度测试
        Private Sub WriteTapeFileSpeed(filePath As String, tapedrive As String)
            Dim sense As Byte()
            TapeUtils.Locate(tapedrive, 0, 1, TapeUtils.LocateDestType.EOD)
            Dim BufferSize As Integer = 16777216
            Dim fs = New IO.FileStream(filePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read, BufferSize, True)
            Dim bytesRead As Integer
            Dim PreReadBuffer(BufferSize - 1) As Byte
            Do
                bytesRead = fs.Read(PreReadBuffer, 0, BufferSize)
                Metric.OperationCounter.WithLabels(("read_file")).Inc(bytesRead)
                ' 处理读取到的数据
                If bytesRead > 0 Then
                    Dim succ As Boolean
                    Dim offset As Integer = 0 ' 数据偏移量
                    Dim plabelblocksize = 524288
                    Dim buffer(plabelblocksize - 1) As Byte
                    While offset < bytesRead ' 只处理实际读取的字节数

                        ' 将 PreReadBuffer 中的数据复制到 buffer 中
                        Array.Copy(PreReadBuffer, offset, buffer, 0, Math.Min(plabelblocksize, bytesRead - offset))

                        sense = TapeUtils.Write(tapedrive, buffer, Math.Min(plabelblocksize, bytesRead - offset))
                        Metric.OperationCounter.WithLabels(("write_file")).Inc(plabelblocksize)
                        If ((sense(2) >> 6) And &H1) = 1 Then
                            If (sense(2) And &HF) = 13 Then
                                '磁带已满                                           
                                Console.WriteLine("磁带已满")
                            Else
                                '磁带即将写满
                                Console.WriteLine("磁带即将写满")
                            End If
                        ElseIf sense(2) And &HF <> 0 Then
                            '写入出错
                            Console.WriteLine("写入出错")
                        Else
                            succ = True
                        End If
                        offset += plabelblocksize ' 更新偏移量
                    End While
                End If

            Loop While bytesRead > 0

            ' 关闭文件流
            fs.Close()
        End Sub
        Public Sub StartUp(args As String())
            Dim argsCollection As New ReadOnlyCollection(Of String)(args)
            Dim startupArgs As New StartupEventArgs(argsCollection)
            MyApplication_Startup(Nothing, startupArgs)
        End Sub

        Private Sub MyApplication_Startup(sender As Object, e As StartupEventArgs)
            If IO.File.Exists(IO.Path.Combine(System.Windows.Forms.Application.StartupPath, "lang.ini")) Then
                Try
                    Dim lang As String = IO.File.ReadAllText(IO.Path.Combine(System.Windows.Forms.Application.StartupPath, "lang.ini"))
                    Threading.Thread.CurrentThread.CurrentCulture = New Globalization.CultureInfo(lang)
                    Threading.Thread.CurrentThread.CurrentUICulture = New Globalization.CultureInfo(lang)
                Catch ex As Exception

                End Try
            End If
            My.Settings.License = Resources.StrDefaultLicense
            If IO.File.Exists(IO.Path.Combine(System.Windows.Forms.Application.StartupPath, "license.key")) Then
                Dim rsa As New System.Security.Cryptography.RSACryptoServiceProvider()

                If IO.File.Exists(IO.Path.Combine(System.Windows.Forms.Application.StartupPath, "privkey.xml")) Then
                    rsa.FromXmlString(IO.File.ReadAllText(IO.Path.Combine(System.Windows.Forms.Application.StartupPath, "privkey.xml")))
                Else
                    rsa.FromXmlString("<RSAKeyValue><Modulus>4q9IKAIqJVyJteY0L7mCVnuBvNv+ciqlJ79X8RdTOzAOsuwTrmdlXIJn0dNsY0EdTNQrJ+idmAcMzIDX65ZnQzMl9x2jfvLZfeArqzNYERkq0jpa/vwdk3wfqEUKhBrGzy14gt/tawRXp3eBGZSEN++Wllh8Zqf8Huiu6U+ZO9k=</Modulus><Exponent>AQAB</Exponent></RSAKeyValue>")
                End If
                Dim lic_string = IO.File.ReadAllText(IO.Path.Combine(Windows.Forms.Application.StartupPath, "license.key"))

                Try
                    Dim LicStr As String() = lic_string.Split({vbCr, vbLf}, StringSplitOptions.RemoveEmptyEntries)
                    Dim strBody As String = LicStr(0)
                    Dim strSign As String = LicStr(1)
                    Dim bSign As Byte() = Convert.FromBase64String(strSign)
                    Dim bLicStr As Byte() = Convert.FromBase64String(strBody)
                    'lic_string = System.Text.Encoding.UTF8.GetString(rsa.Decrypt(key, False))
                    If rsa.VerifyData(bLicStr, "SHA256", bSign) Then
                        My.Settings.License = System.Text.Encoding.UTF8.GetString(bLicStr)
                    Else
                        Throw New Exception()
                    End If
                Catch ex As Exception
                    If rsa.PublicOnly Then
                        MessageBox.Show(Resources.StrLicenseInvalid)
                    Else
                        My.Settings.License = lic_string
                        Dim bLicStr As Byte() = System.Text.Encoding.UTF8.GetBytes(lic_string)
                        Dim bSign As Byte() = rsa.SignData(bLicStr, "SHA256")
                        Dim strBody As String = Convert.ToBase64String(bLicStr)
                        Dim strSign As String = Convert.ToBase64String(bSign)
                        IO.File.WriteAllText(IO.Path.Combine(Windows.Forms.Application.StartupPath, "license.key"), $"{strBody}{vbCrLf}{strSign}")
                    End If
                End Try
            End If
            If e.CommandLine.Count = 0 Then

            Else
                Dim param() As String = e.CommandLine.ToArray()
                Dim IndexRead As Boolean = True
                For i As Integer = 0 To param.Count - 1
                    If param(i).StartsWith("/") Then param(i) = "-" & param(i).TrimStart("/")

                    Select Case param(i)
                        Case "-s"
                            IndexRead = False
                        Case "-readtest"
                            Dim server As MetricServer = New MetricServer(My.Settings.LTFSWriter_PrometheusPort)
                            Try
                                CheckUAC(e)
                                server.Start()
                            Catch ex As Exception
                                Console.WriteLine(ex.Message)
                                MessageBox.Show(ex.Message)
                            End Try
                            StackTraceAnalysis.StartServer()
'                            ReadFileSpeed("M:\\parlorpcbakdpan\\VHD\\PARLOR-PC.VHD")
                            '                            WriteTapeFileSpeed("M:\\parlorpcbakdpan\\VHD\\PARLOR-PC.VHD","\\.\TAPE0")
                           WriteTapeFileWithEfuSpeed("W:\\临时Vhd\\M盘小文件读取测试.efu", "\\.\TAPE0")
'                             MountTape("\\.\TAPE0")
                        Case "-t"
                            CheckUAC(e)
                            If i < param.Count - 1 Then
                                Dim TapeDrive As String = param(i + 1)
                                If TapeDrive.StartsWith("TAPE") Then
                                    TapeDrive = "\\.\" & TapeDrive
                                ElseIf TapeDrive.StartsWith("\\.\") Then
                                    'Do Nothing
                                ElseIf TapeDrive = Val(TapeDrive).ToString Then
                                    TapeDrive = "\\.\TAPE" & TapeDrive
                                Else

                                End If
                                If My.Settings.LTFSWriter_PrometheusPort > 0 Then
                                    Dim server As MetricServer = New MetricServer("localhost", Convert.ToInt32(My.Settings.LTFSWriter_PrometheusPort))
                                    Try
                                        server.Start()
                                    Catch ex As Exception
                                        Console.WriteLine(ex.Message)
                                        MessageBox.Show(ex.Message)
                                    End Try
                                    StackTraceAnalysis.StartServer()
                                End If

                                Dim LWF As New LTFSWriter With {.TapeDrive = TapeDrive, .OfflineMode = Not IndexRead}
                                If Not IndexRead Then LWF.ExtraPartitionCount = 1
                                Me.MainForm = LWF
                                Exit For

                            End If
                        Case "-f"
                            If i < param.Count - 1 Then
                                Dim indexFile As String = param(i + 1).TrimStart("""").TrimEnd("""")

                                If IO.File.Exists(indexFile) Then
                                    Dim LWF As New LTFSWriter With {.Barcode = Resources.StrIndexView, .TapeDrive = "", .OfflineMode = True}
                                    Dim OnLWFLoad As New EventHandler(Sub()
                                                                          LWF.Invoke(Sub()
                                                                                         LWF.LoadIndexFile(indexFile, True)
                                                                                         LWF.ToolStripStatusLabel1.Text = Resources.StrIndexView
                                                                                     End Sub)
                                                                          RemoveHandler LWF.Load, OnLWFLoad
                                                                      End Sub
                                        )
                                    AddHandler LWF.Load, OnLWFLoad
                                    Me.MainForm = LWF
                                End If
                                Exit For
                            End If
                        Case "-c"
                            CheckUAC(e)
                            Me.MainForm = LTFSConfigurator
                            Exit For
                        Case "-rb"
                            CheckUAC(e)
                            InitConsole()
                            If i < param.Count - 1 Then
                                Dim TapeDrive As String = param(i + 1)
                                If TapeDrive.StartsWith("TAPE") Then
                                    TapeDrive = "\\.\" & TapeDrive
                                ElseIf TapeDrive.StartsWith("\\.\") Then
                                    'Do Nothing
                                ElseIf TapeDrive = Val(TapeDrive).ToString Then
                                    TapeDrive = "\\.\TAPE" & TapeDrive
                                Else

                                End If
                                Dim Barcode As String = TapeUtils.ReadBarcode(TapeDrive)
                                Console.WriteLine($"{TapeDrive}{vbCrLf}Barcode:{Barcode}")
                                CloseConsole()
                                End
                            End If
                        Case "-wb"
                            CheckUAC(e)
                            InitConsole()
                            If i < param.Count - 2 Then
                                Dim TapeDrive As String = param(i + 1)
                                If TapeDrive.StartsWith("TAPE") Then
                                    TapeDrive = "\\.\" & TapeDrive
                                ElseIf TapeDrive.StartsWith("\\.\") Then
                                    'Do Nothing
                                ElseIf TapeDrive = Val(TapeDrive).ToString Then
                                    TapeDrive = "\\.\TAPE" & TapeDrive
                                Else

                                End If
                                Dim Barcode As String = param(i + 2)
                                If TapeUtils.SetBarcode(TapeDrive, Barcode) Then
                                    Console.WriteLine($"{TapeDrive}{vbCrLf}Barcode->{TapeUtils.ReadBarcode(TapeDrive)}")
                                Else
                                    Console.WriteLine($"{TapeDrive}{vbCrLf}{Resources.StrBCSFail}")
                                End If
                                CloseConsole()
                                End
                            End If
                        Case "-raw"
                            CheckUAC(e)
                            InitConsole()
                            If i < param.Count - 4 Then
                                Dim TapeDrive As String = param(i + 1)
                                If TapeDrive.StartsWith("TAPE") Then
                                    TapeDrive = "\\.\" & TapeDrive
                                ElseIf TapeDrive.StartsWith("\\.\") Then
                                    'Do Nothing
                                ElseIf TapeDrive = Val(TapeDrive).ToString Then
                                    TapeDrive = "\\.\TAPE" & TapeDrive
                                Else

                                End If
                                Dim cdb As Byte() = LTFSConfigurator.HexStringToByteArray(param(i + 2))
                                Dim data As Byte() = LTFSConfigurator.HexStringToByteArray(param(i + 3))
                                Dim dataDir As Integer = Val(param(i + 4))
                                Dim TimeOut As Integer = 60000
                                If i + 5 <= param.Length - 1 Then
                                    TimeOut = Val(param(i + 5))
                                End If
                                Dim sense As Byte() = {}

                                If TapeUtils.SendSCSICommand(TapeDrive, cdb, data, dataDir, Function(s As Byte())
                                                                                                sense = s
                                                                                                Return True
                                                                                            End Function, TimeOut) Then
                                    Console.WriteLine($"{TapeDrive}
cdb:
{TapeUtils.Byte2Hex(cdb)}
param:
{TapeUtils.Byte2Hex(data)}
dataDir:{dataDir}

{Resources.StrSCSISucc}
sense:
{TapeUtils.Byte2Hex(sense)}
{TapeUtils.ParseSenseData(sense)}")
                                Else
                                    Console.WriteLine($"{TapeDrive}
cdb:
{TapeUtils.Byte2Hex(cdb)}
param:
{TapeUtils.Byte2Hex(data)}
dataDir:{dataDir}


{Resources.StrSCSIFail}")
                                End If

                                CloseConsole()
                                End
                            End If
                        Case "-mkltfs"
                            CheckUAC(e)
                            InitConsole()
                            'Console.WriteLine($"{i} {param.Count}")
                            If i < param.Count - 1 Then
                                Dim TapeDrive As String = param(i + 1)
                                If TapeDrive.StartsWith("TAPE") Then
                                    TapeDrive = "\\.\" & TapeDrive
                                ElseIf TapeDrive.StartsWith("\\.\") Then
                                    'Do Nothing
                                ElseIf TapeDrive = Val(TapeDrive).ToString Then
                                    TapeDrive = "\\.\TAPE" & TapeDrive
                                Else

                                End If
                                Dim Barcode As String = ""
                                If i + 2 <= param.Length - 1 Then
                                    Barcode = param(i + 2).Replace("""", "")
                                    If Barcode.Length > 20 Then Barcode = Barcode.Substring(0, 20)
                                End If

                                Dim VolLabel As String = ""
                                If i + 3 <= param.Length - 1 Then
                                    VolLabel = param(i + 3).Replace("""", "")
                                End If

                                Dim Partition As Byte = 1
                                If i + 4 <= param.Length - 1 Then
                                    Partition = Partition And Val(param(i + 4))
                                End If

                                Dim Capacity As UInt16 = &HFFFF
                                If i + 5 <= param.Length - 1 Then
                                    Capacity = Capacity And Val(param(i + 5))
                                End If
                                Dim BlockLen As Integer = 524288
                                If i + 6 <= param.Length - 1 Then
                                    BlockLen = Val(param(i + 6))
                                End If
                                Dim P0Size As UInt16 = 1
                                If i + 7 <= param.Length - 1 Then
                                    P0Size = &HFFFF And Val(param(i + 7))
                                End If
                                Dim P1Size As UInt16 = &HFFFF
                                If i + 8 <= param.Length - 1 Then
                                    P1Size = &HFFFF And Val(param(i + 8))
                                End If

                                Dim sense As Byte() = {}
                                If TapeUtils.mkltfs(TapeDrive, Barcode, VolLabel, Partition, BlockLen, True,
                                    Sub(s As String)
                                        'ProgReport
                                        Console.WriteLine(s)
                                    End Sub,
                                    Sub(s As String)
                                        'OnFin
                                        Console.WriteLine(s)
                                    End Sub,
                                    Sub(s As String)
                                        'OnErr
                                        Console.WriteLine(s)
                                    End Sub, Capacity, P0Size, P1Size) Then
                                    Console.WriteLine(Resources.StrFormatFin)
                                Else
                                    Console.WriteLine(Resources.StrFormatError)
                                End If
                                CloseConsole()
                                End
                            End If
                        Case "-copy"
                            CheckUAC(e)
                            Me.MainForm = New TapeCopy()

                        Case "-gt"
                            If i < param.Count - 2 Then
                                Dim Num1 As Byte = Byte.Parse(param(i + 1))
                                Dim Num2 As Byte = Byte.Parse(param(i + 2))
                                InitConsole()
                                Console.WriteLine($"{TapeUtils.GX256.Times(Num1, Num2)}")
                                CloseConsole()
                                End
                            End If
                        Case "-crc"
                            If i < param.Count - 1 Then
                                Dim Num1 As Byte() = LTFSConfigurator.HexStringToByteArray(param(i + 1))
                                InitConsole()
                                Console.WriteLine($"{TapeUtils.Byte2Hex(TapeUtils.GX256.CalcCRC(Num1))}")
                                CloseConsole()
                                End
                            End If
                        Case "-lic"
                            If i < param.Count - 1 Then
                                Dim ltext As String = param(i + 1)
                                If ltext.StartsWith("""") AndAlso ltext.EndsWith("""") Then
                                    ltext = ltext.Substring(1, ltext.Length - 2)
                                End If
                                InitConsole()
                                Dim rsa As New System.Security.Cryptography.RSACryptoServiceProvider()

                                If IO.File.Exists(IO.Path.Combine(System.Windows.Forms.Application.StartupPath, "privkey.xml")) Then
                                    rsa.FromXmlString(IO.File.ReadAllText(IO.Path.Combine(System.Windows.Forms.Application.StartupPath, "privkey.xml")))
                                Else
                                    rsa.FromXmlString("<RSAKeyValue><Modulus>4q9IKAIqJVyJteY0L7mCVnuBvNv+ciqlJ79X8RdTOzAOsuwTrmdlXIJn0dNsY0EdTNQrJ+idmAcMzIDX65ZnQzMl9x2jfvLZfeArqzNYERkq0jpa/vwdk3wfqEUKhBrGzy14gt/tawRXp3eBGZSEN++Wllh8Zqf8Huiu6U+ZO9k=</Modulus><Exponent>AQAB</Exponent></RSAKeyValue>")
                                End If
                                If rsa.PublicOnly Then
                                    MessageBox.Show(Resources.StrLicenseInvalid)
                                Else
                                    My.Settings.License = ltext
                                    Dim bLicStr As Byte() = System.Text.Encoding.UTF8.GetBytes(ltext)
                                    Dim bSign As Byte() = rsa.SignData(bLicStr, "SHA256")
                                    Dim strBody As String = Convert.ToBase64String(bLicStr)
                                    Dim strSign As String = Convert.ToBase64String(bSign)
                                    Console.WriteLine($"{strBody}{vbCrLf}{strSign}")
                                End If
                                CloseConsole()
                                End
                            End If
                        Case Else
                            Try
                                InitConsole()
                                Console.WriteLine($"LTFSCopyGUI v{My.Application.Info.Version.ToString(3)}{My.Settings.License}
{Resources.StrCMDHelpText}")

                                CloseConsole()
                                End

                            Catch ex As Exception
                                MessageBox.Show(ex.ToString)
                            End Try
                            'End
                    End Select
                Next
            End If
            If Me.MainForm is Nothing Then
                    Me.MainForm = New LTFSConfigurator()
            End If
            System.Windows.Forms.Application.EnableVisualStyles()
            System.Windows.Forms.Application.Run(Me.MainForm)
        End Sub
    End Class
End Namespace
