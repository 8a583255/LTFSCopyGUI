Module My1
    Public Sub Main(args As String())
        System.Windows.Forms.Application.SetCompatibleTextRenderingDefault(False)
        Dim my As New My.MyApplication()
        my.StartUp(args)
    End Sub
End Module
