Imports System.Reflection
Imports Castle.DynamicProxy
Imports Newtonsoft.Json

Public Class LoggingInterceptor
    Implements IInterceptor
    Private Function PrintParameters(invocation As IInvocation) as Dictionary(Of String, Object)
        Dim parametersDict As New Dictionary(Of String, Object)()
        ' 遍历参数数组并添加到字典中
        For i As Integer = 0 To invocation.Arguments.Length - 1
            Dim argument As Object = invocation.Arguments(i)
            Dim parameterInfo As ParameterInfo = invocation.Method.GetParameters()(i)
            Dim Out As string = ""
            if parameterInfo.IsOut Then
                Out = "Out_"
            End If
            Try
                ' 添加参数到字典
'                 parametersDict.Add($"{Out}{parameterInfo.Name}",  JsonConvert.DeserializeObject(JsonConvert.SerializeObject(argument)))
'                parametersDict.Add($"{Out}{parameterInfo.Name}", Convert.ToString(argument))
            Catch ex As Exception
                parametersDict.Add($"{Out}{parameterInfo.Name}", ex.Message & ex. StackTrace)
            End Try
        Next
        Return parametersDict
    End Function
    Public Sub Intercept(invocation As IInvocation) Implements IInterceptor.Intercept



        ' 在调用之前执行预处理逻辑
'        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} 【{invocation.Method.Name} 】开始 : {JsonConvert.SerializeObject(PrintParameters(invocation))}")
'        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} 【{invocation.Method.Name} 】开始 : ")

        Dim startTimestamp4 = DateTime.Now
        ' 调用原始方法
        try
            invocation.Proceed()
        Catch ex As Exception
            Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} 【{invocation.Method.Name} 】异常 : {ex.Message}: {JsonConvert.SerializeObject(PrintParameters(invocation))}")
        End Try
        Dim duration4 As TimeSpan = DateTime.Now - startTimestamp4
        Metric.FileOperationDurationHistogram.WithLabels("", "Intercept_" & invocation.Method.Name, "big").Observe(duration4.TotalMilliseconds)
        Metric.FileOperationDurationSummary.WithLabels("", "Intercept_" & invocation.Method.Name, "big").Observe(duration4.TotalMilliseconds)
        '
        ' 在调用之后执行后处理逻辑
'        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} 【{invocation.Method.Name} 】结束:  {JsonConvert.SerializeObject(PrintParameters(invocation))} return {JsonConvert.SerializeObject(invocation.ReturnValue)}")
'        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} 【{invocation.Method.Name} 】结束: ")
    End Sub
End Class
