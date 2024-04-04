Imports System
Imports System.Collections.Generic
Imports System.Diagnostics
Imports System.IO
Imports System.Linq
Imports System.Net
Imports System.Text
Imports System.Threading
Imports System.Web
Imports System.Web.Script.Serialization
Imports System.Web.Security
Imports System.Web.SessionState
Imports Microsoft.Diagnostics.Runtime
Imports System.Threading.Tasks

Public Module StackTraceAnalysis

    Public Sub StartServer()
        Dim listener = New HttpListener()
        listener.Prefixes.Add("http://localhost:8081/") ' 设置要监听的地址和端口
        listener.Start()

        Console.WriteLine("正在等待请求...")

        Task.Run(Sub() ProcessRequests(listener)) ' 在独立线程中运行进程
    End Sub

    Private Sub ProcessRequests(ByVal listener As HttpListener)
        While listener.IsListening
            Dim context = listener.GetContext()
            Task.Run(Sub() ProcessRequest(context)) ' 在独立线程中处理请求
        End While
    End Sub

    Private Sub ProcessRequest(ByVal context As HttpListenerContext)
        ' 将请求处理逻辑放在这里，例如返回响应内容等操作
        Dim responseString As String = GetAllStackTraces()

        Dim buffer As Byte() = System.Text.Encoding.GetEncoding("GBK").GetBytes(responseString)
        context.Response.ContentLength64 = buffer.Length
        context.Response.OutputStream.Write(buffer, 0, buffer.Length)
        context.Response.OutputStream.Close()

        context.Response.Close()
    End Sub

    Public Function GetAllStackTraces() As String
        Try
            Dim threadLogDic As New Dictionary(Of String, Integer)()
            Dim threads = New List(Of ThreadInfo1)()

            Using target = DataTarget.CreateSnapshotAndAttach(Process.GetCurrentProcess().Id)
                Dim runtime = target.ClrVersions.First().CreateRuntime()

                ' We can't get the thread name from the ClrThead objects, so we'll look for
                ' Thread instances on the heap and get the names from those.    
                For Each thread In runtime.Threads
                    Dim t = thread

                    Dim stack As String = ""
                    For Each clrStackFrame In thread.EnumerateStackTrace()
                        stack += $"{clrStackFrame.Method}" & vbLf
                    Next

                    threads.Add(New ThreadInfo1() With {
                        .Address = t.Address,
                        .ManagedThreadId = t.ManagedThreadId,
                        .OSThreadId = t.OSThreadId,
                        .IsAborted = Nothing,
                        .IsAlive = t.IsAlive,
                        .IsBackground = Nothing,
                        .IsGC = t.IsGc,
                        .IsThreadpoolGate = Nothing,
                        .IsThreadpoolTimer = Nothing,
                        .IsThreadpoolWait = Nothing,
                        .IsThreadpoolWorker = Nothing,
                        .IsThreadpoolCompletionPort = Nothing,
                        .Stack = stack
                    })
                Next
            End Using

            Dim stackDic = threads.GroupBy(Function(t) t.Stack).ToDictionary(Function(t) t.Key, Function(t) t.Count())

            'Return (New JavaScriptSerializer().Serialize(New With {
            '.StackDic = stackDic,
            '.IsWorkerThreadDic = threads.GroupBy(Function(t) t.IsThreadpoolWorker).ToDictionary(Function(t) t.Key, Function(t) t.Count()).ToList(),
            '.IsAliveThreadDic = threads.GroupBy(Function(t) t.IsAlive).ToDictionary(Function(t) t.Key, Function(t) t.Count()).ToList(),
            '.IsThreadpoolCompletionPortThreadDic = threads.GroupBy(Function(t) t.IsThreadpoolCompletionPort).ToDictionary(Function(t) t.Key, Function(t) t.Count()).ToList()
            '}))
            Dim output As New StringBuilder()
            output.AppendLine("线程总数: " & threads.Count)

            For Each stack In stackDic
                output.AppendLine($"线程数: {stack.Value}   {stack.Key}")
            Next

            Return output.ToString()
        Catch e As Exception
            Console.WriteLine(e)
            Return e.Message & e.StackTrace
        End Try

        Return "err"
    End Function

End Module

Public Class ThreadInfo1
    Public Property Address As ULong
    Public Property ManagedThreadId As Integer
    Public Property Stack As String
    Public Property IsThreadpoolCompletionPort As Boolean
    Public Property IsThreadpoolWorker As Boolean
    Public Property IsThreadpoolWait As Boolean
    Public Property OSThreadId As UInteger
    Public Property IsAborted As Boolean
    Public Property IsAlive As Boolean
    Public Property IsBackground As Boolean
    Public Property IsGC As Boolean
    Public Property IsThreadpoolGate As Boolean
    Public Property IsThreadpoolTimer As Boolean
End Class
