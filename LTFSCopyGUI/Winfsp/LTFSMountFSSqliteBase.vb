Imports System.Collections.Concurrent
Imports System.IO
Imports System.Runtime.InteropServices
Imports System.Security.AccessControl
Imports System.Threading
Imports Fsp.Interop
Imports Microsoft.VisualBasic.CompilerServices
Imports Newtonsoft.Json

Public Class LTFSMountFSSqliteBase
    Inherits Fsp.FileSystemBase
    Public LW As LTFSWriter
    Public VolumeLabel As String
    Public TapeDrive As String
    Public Const ALLOCATION_UNIT As Integer = 4096

    Protected Shared Sub ThrowIoExceptionWithHResult(ByVal HResult As Int32)
        Throw New IO.IOException(Nothing, HResult)
    End Sub

    Protected Shared Sub ThrowIoExceptionWithWin32(ByVal [Error] As Int32)
        ThrowIoExceptionWithHResult(CType((2147942400 Or [Error]), Int32))
        'TODO: checked/unchecked is not supported at this time
    End Sub

    Protected Shared Sub ThrowIoExceptionWithNtStatus(ByVal Status As Int32)
        ThrowIoExceptionWithWin32(CType(Win32FromNtStatus(Status), Int32))
    End Sub

    Public Overrides Function ExceptionHandler(ByVal ex As Exception) As Int32
        Dim HResult As Int32 = ex.HResult
        Console.WriteLine(ex.Message & ex.StackTrace)
        If (2147942400 _
            = (HResult And 4294901760)) Then
            Return NtStatusFromWin32((CType(HResult, UInt32) And 65535))
        End If
        Return STATUS_UNEXPECTED_IO_ERROR
    End Function

    Class FileDesc
        Public IsDirectory As Boolean
        Public UnwriteFile As LTFSWriter.FileRecord
        Public LTFSFile As ltfsindex.file
        Public LTFSDirectory As ltfsindex.directory
        Public Parent As ltfsindex.directory
        Public Position As TapeUtils.PositionData
        Public Sh As IOManager.CheckSumBlockwiseCalculator
        Public FileSystemInfos() As DictionaryEntry
        Public Property OperationId As String


        Public Enum dwFilAttributesValue As UInteger
            FILE_ATTRIBUTE_ARCHIVE = &H20
            FILE_ATTRIBUTE_COMPRESSED = &H800
            FILE_ATTRIBUTE_DIRECTORY = &H10
            FILE_ATTRIBUTE_ENCRYPTED = &H4000
            FILE_ATTRIBUTE_HIDDEN = &H2
            FILE_ATTRIBUTE_NORMAL = &H80
            FILE_ATTRIBUTE_OFFLINE = &H1000
            FILE_ATTRIBUTE_READONLY = &H1
            FILE_ATTRIBUTE_REPARSE_POINT = &H400
            FILE_ATTRIBUTE_SPARSE_FILE = &H200
            FILE_ATTRIBUTE_SYSTEM = &H4
            FILE_ATTRIBUTE_TEMPORARY = &H100
            FILE_ATTRIBUTE_VIRTUAL = &H10000
        End Enum

        Public Function GetFileInfo(ByRef FileInfo As Fsp.Interop.FileInfo) As Int32
'            Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} FileDesc. GetFileInfo({FileInfo})")
            If (Not IsDirectory) Then
                FileInfo.FileAttributes = dwFilAttributesValue.FILE_ATTRIBUTE_OFFLINE Or
                                          dwFilAttributesValue.FILE_ATTRIBUTE_ARCHIVE
                If LTFSFile.readonly Then _
                    FileInfo.FileAttributes = FileInfo.FileAttributes Or dwFilAttributesValue.FILE_ATTRIBUTE_READONLY
                FileInfo.ReparseTag = 0
                FileInfo.FileSize = LTFSFile.length
                FileInfo.AllocationSize = (FileInfo.FileSize + ALLOCATION_UNIT - 1)/ALLOCATION_UNIT*ALLOCATION_UNIT
                FileInfo.CreationTime = TapeUtils.ParseTimeStamp(LTFSFile.creationtime).ToFileTimeUtc
                FileInfo.LastAccessTime = TapeUtils.ParseTimeStamp(LTFSFile.accesstime).ToFileTimeUtc
                FileInfo.LastWriteTime = TapeUtils.ParseTimeStamp(LTFSFile.changetime).ToFileTimeUtc
                FileInfo.ChangeTime = TapeUtils.ParseTimeStamp(LTFSFile.changetime).ToFileTimeUtc
                FileInfo.IndexNumber = 0
                FileInfo.HardLinks = 0
            Else
                FileInfo.FileAttributes = dwFilAttributesValue.FILE_ATTRIBUTE_DIRECTORY
                FileInfo.ReparseTag = 0
                FileInfo.FileSize = 0
                FileInfo.AllocationSize = 0
                FileInfo.CreationTime = TapeUtils.ParseTimeStamp(LTFSDirectory.creationtime).ToFileTimeUtc
                FileInfo.LastAccessTime = TapeUtils.ParseTimeStamp(LTFSDirectory.accesstime).ToFileTimeUtc
                FileInfo.LastWriteTime = TapeUtils.ParseTimeStamp(LTFSDirectory.changetime).ToFileTimeUtc
                FileInfo.ChangeTime = TapeUtils.ParseTimeStamp(LTFSDirectory.changetime).ToFileTimeUtc
                FileInfo.IndexNumber = 0
                FileInfo.HardLinks = 0
            End If
            Return STATUS_SUCCESS
        End Function

        Public Function GetFileAttributes() As UInt32
            Dim FileInfo As Fsp.Interop.FileInfo
            Me.GetFileInfo(FileInfo)
            Return FileInfo.FileAttributes
        End Function
    End Class


    Public Overrides Function Init(Host0 As Object) As Integer
        Dim Host As Fsp.FileSystemHost = CType(Host0, Fsp.FileSystemHost)
        Try
            Host.FileInfoTimeout = 10 * 1000
            Host.FileSystemName = "LTFS"
            Host.SectorSize = 4096
            Host.SectorsPerAllocationUnit = 1 'LW.plabel.blocksize\Host.SectorSize
            Host.VolumeCreationTime = TapeUtils.ParseTimeStamp(LW.plabel.formattime).ToFileTimeUtc()
            Host.VolumeSerialNumber = 0
            Host.CaseSensitiveSearch = False
            Host.CasePreservedNames = True
            Host.UnicodeOnDisk = True
            Host.PersistentAcls = True
            '            Host.ReparsePoints = False
            '            Host.ReparsePointsAccessCheck = False
            '            Host.NamedStreams = False
            Host.PostCleanupWhenModifiedOnly = True
            '            Host.FlushAndPurgeOnCleanup = True
            Host.PassQueryDirectoryPattern = True
            Host.MaxComponentLength = 4096

        Catch ex As Exception
            MessageBox.Show(ex.ToString)
        End Try
        StartRefreshThread()
        Return STATUS_SUCCESS
    End Function

    Private Class DirectoryEntryComparer
        Implements IComparer

        Public Function Compare(ByVal x As Object, ByVal y As Object) As Integer Implements IComparer.Compare
            Return _
                String.Compare(CType(CType(x, DictionaryEntry).Key, String),
                               CType(CType(y, DictionaryEntry).Key, String))
        End Function
    End Class

    Dim _DirectoryEntryComparer As DirectoryEntryComparer = New DirectoryEntryComparer

    Public Sub New(path0 As String)
        TapeDrive = path0
    End Sub

    Public Sub New()
    End Sub

    Public Shared TotalSize As ULong
    Public Shared FreeSize As ULong

    Public Overrides Function GetVolumeInfo(<Out> ByRef VolumeInfo As VolumeInfo) As Int32
        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}  【GetVolumeInfo】(${VolumeInfo})")
        VolumeInfo = New VolumeInfo()
        VolumeLabel = LW.schema._directory(0).name
        Try
            If TotalSize = 0 Then
                VolumeInfo.TotalSize =
                    TapeUtils.MAMAttribute.FromTapeDrive(LW.TapeDrive, 0, 1, LW.ExtraPartitionCount).AsNumeric << 20
                VolumeInfo.FreeSize =
                    TapeUtils.MAMAttribute.FromTapeDrive(LW.TapeDrive, 0, 0, LW.ExtraPartitionCount).AsNumeric << 20
                'VolumeInfo.SetVolumeLabel(VolumeLabel)
                TotalSize = VolumeInfo.TotalSize
                FreeSize = VolumeInfo.FreeSize
            Else
                VolumeInfo.TotalSize = TotalSize
                VolumeInfo.FreeSize = FreeSize
            End If
        Catch ex As Exception
            MessageBox.Show(ex.ToString)
        End Try
        Return STATUS_SUCCESS
    End Function

    Public Function getecurityDescriptor() As Byte()
        Dim securityDescriptor As RawSecurityDescriptor =
                New RawSecurityDescriptor("O:BAG:BAD:P(A;;FA;;;SY)(A;;FA;;;BA)(A;;FA;;;WD)")
        Dim result As Byte() = New Byte(securityDescriptor.BinaryLength - 1) {}
        securityDescriptor.GetBinaryForm(result, 0)
        Return result
    End Function

    Public Overrides Function GetSecurityByName(FileName As String, ByRef FileAttributes As UInteger,
                                                ByRef SecurityDescriptor() As Byte) As Integer
        '        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}  【GetSecurityByName】(${FileName})")
        SecurityDescriptor = getecurityDescriptor()
        If LW.schema._directory.Count = 0 Then Throw New Exception("Not LTFS formatted")
        Dim path As String() = FileName.Split({"\"}, StringSplitOptions.RemoveEmptyEntries)
        Dim filedesc As FileDesc
        Dim FileInfo As New Fsp.Interop.FileInfo
        If path.Length = 0 Then
            filedesc = New FileDesc _
                With {.IsDirectory = True, .LTFSDirectory = new ltfsindex.directory() With{
                     .name = "",
                      .fullpath="/",
                    .accesstime= LW.plabel.formattime,
                    .creationtime= LW.plabel.formattime,
                    .changetime= LW.plabel.formattime,
                    .modifytime= LW.plabel.formattime
                    },
                    .OperationId = Guid.NewGuid().ToString()}
            '            Console.WriteLine(($" New FileDesc GetSecurityByName： {FileName} {filedesc.OperationId}"))
            filedesc.GetFileInfo(FileInfo)
            FileAttributes = FileInfo.FileAttributes
            Return STATUS_SUCCESS
        End If
        Dim FileExist As Boolean = False
        Dim LTFSDirOrFile = DirProvider.QueryFileWithWhere($"fullpath='{FileName}'", LW.GetSqliteConnection(LW.Barcode))
        If LTFSDirOrFile.Count=0 Then
            FileExist = False
        else
            FileExist = True
            if LTFSDirOrFile(0).isDirectory then 
                filedesc = New FileDesc With {.IsDirectory = True, .LTFSDirectory = DirProvider.ConvertFileToDir(LTFSDirOrFile(0)), .OperationId = Guid.NewGuid().ToString()}
            Else 
                filedesc = New FileDesc With {.IsDirectory = False, .LTFSFile = LTFSDirOrFile(0),.OperationId = Guid.NewGuid().ToString()}
            End If
        End If

        If FileExist Then
            filedesc.GetFileInfo(FileInfo)
        End If
        FileAttributes = FileInfo.FileAttributes
        If FileExist Then
            Return STATUS_SUCCESS
        Else
            Return STATUS_OBJECT_NAME_NOT_FOUND
        End If
    End Function

    Public Overrides Function Open(FileName As String,
                                   CreateOptions As UInteger,
                                   GrantedAccess As UInteger,
                                   ByRef FileNode As Object,
                                   ByRef FileDesc0 As Object,
                                   ByRef FileInfo As Fsp.Interop.FileInfo,
                                   ByRef NormalizedName As String) As Integer
        Dim OperationId As String
        Dim fileDesc = DirectCast(FileDesc0, FileDesc)
        If Not fileDesc Is Nothing Then
            If Not fileDesc.OperationId Is Nothing Then
                OperationId = fileDesc.OperationId
            End If
        End If
        '        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}  Open({FileName}) OperationId:{OperationId} CreateOptions:{CreateOptions} GrantedAccess:{GrantedAccess}")
        NormalizedName = FileName
        Try
            'FileNode = New Object()
            If LW.schema._directory.Count = 0 Then Throw New Exception("Not LTFS formatted")
            Dim path As String() = FileName.Split({"\"}, StringSplitOptions.RemoveEmptyEntries)
            If path.Length = 0 Then
                fileDesc = New FileDesc _
                    With {.IsDirectory = True, .LTFSDirectory = LW.schema._directory(0),
                        .OperationId = Guid.NewGuid().ToString()}
                '                Console.WriteLine(($" New FileDesc Open： {FileName} {fileDesc.OperationId}"))
                Dim status As Integer = CType(fileDesc, FileDesc).GetFileInfo(FileInfo)
                FileDesc0 = fileDesc
                Return status
            End If
            Dim FileExist As Boolean = False
            If path.Length = 0 Then
                fileDesc = New FileDesc With {.IsDirectory = True, .LTFSDirectory = LW.schema._directory(0),.OperationId = Guid.NewGuid().ToString()}
                '            Console.WriteLine(($" New FileDesc GetSecurityByName： {FileName} {filedesc.OperationId}"))
                fileDesc.GetFileInfo(FileInfo)
                Return STATUS_SUCCESS
            End If
            Dim LTFSDirOrFile = DirProvider.QueryFileWithWhere($"fullpath='{FileName}'", LW.GetSqliteConnection(LW.Barcode))
            If LTFSDirOrFile.Count=0 Then
                FileExist = False
            else
                FileExist = True
                if LTFSDirOrFile(0).isDirectory then 
                    filedesc = New FileDesc With {.IsDirectory = True, .LTFSDirectory = DirProvider.ConvertFileToDir(LTFSDirOrFile(0)), .OperationId = Guid.NewGuid().ToString()}
                Else 
                    filedesc = New FileDesc With {.IsDirectory = False, .LTFSFile = LTFSDirOrFile(0),.OperationId = Guid.NewGuid().ToString()}
                End If
            End If
            If FileExist Then
                Dim status As Integer = CType(fileDesc, FileDesc).GetFileInfo(FileInfo)
                FileDesc0 = fileDesc
                FileInfo = FileInfo
                Return STATUS_SUCCESS
            End If
        Catch ex As Exception
            Console.WriteLine(ex.Message)
            Throw
        End Try
        Return STATUS_NOT_FOUND
    End Function

    Public Overrides Sub Close(FileNode As Object, FileDesc As Object)
        '        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}  【Close()】")
    End Sub

    Public Overrides Function Read(FileNode As Object,
                                   FileDesc0 As Object,
                                   Buffer As IntPtr,
                                   Offset As ULong,
                                   Length As UInteger,
                                   ByRef BytesTransferred As UInteger) As Integer
        Dim FileDesc = DirectCast(FileDesc0, FileDesc)
        Dim OperationId As String
        If Not FileDesc Is Nothing Then
            If Not FileDesc.OperationId Is Nothing Then
                OperationId = FileDesc.OperationId
            End If
        End If
        '        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}  【Read()】 OperationId:{OperationId}")
        If FileDesc Is Nothing OrElse TypeOf FileDesc IsNot FileDesc Then Return STATUS_NOT_FOUND
        Try
            With CType(FileDesc, FileDesc)
                If .IsDirectory Then Return STATUS_NOT_FOUND
                If .LTFSFile Is Nothing Then Return STATUS_NOT_FOUND
                If Offset >= .LTFSFile.length Then ThrowIoExceptionWithNtStatus(STATUS_END_OF_FILE)
                .LTFSFile.extentinfo.Sort(
                    New Comparison(Of ltfsindex.file.extent)(
                        Function(a As ltfsindex.file.extent, b As ltfsindex.file.extent) As Integer
                            Return (a.fileoffset).CompareTo(b.fileoffset)
                        End Function))
                Dim BufferOffset As Long = Offset
                For ei As Integer = 0 To .LTFSFile.extentinfo.Count - 1
                    With .LTFSFile.extentinfo(ei)
                        If Offset >= .fileoffset + .bytecount Then Continue For
                        Dim CurrentFileOffset As Long = .fileoffset
                        Metric.FuncFileOperationDuration(Sub()
                            TapeUtils.Locate(TapeDrive, .startblock, LW.GetPartitionNumber(.partition))
                        End Sub, {"", "Read_Locate", ""})
                        Dim blkBuffer As Byte() = TapeUtils.ReadBlock(TapeDrive)
                        CurrentFileOffset += blkBuffer.Length - .byteoffset
                        While CurrentFileOffset <= Offset
                            blkBuffer = TapeUtils.ReadBlock(TapeDrive)
                            CurrentFileOffset += blkBuffer.Length
                        End While
                        Dim FirstBlockByteOffset As Integer = blkBuffer.Length - (CurrentFileOffset - Offset)
                        Marshal.Copy(blkBuffer, FirstBlockByteOffset, Buffer,
                                     Math.Min(Length, blkBuffer.Length - FirstBlockByteOffset))
                        BufferOffset += Math.Min(Length, blkBuffer.Length - FirstBlockByteOffset)
                        BytesTransferred += Math.Min(Length, blkBuffer.Length - FirstBlockByteOffset)
                        While BufferOffset < .bytecount AndAlso BufferOffset < Length
                            blkBuffer = TapeUtils.ReadBlock(TapeDrive)
                            Marshal.Copy(blkBuffer, 0, New IntPtr(Buffer.ToInt64 + BufferOffset),
                                         Math.Min(Length - BufferOffset,
                                                  Math.Min(blkBuffer.Length, .bytecount - BufferOffset)))
                            BufferOffset += Math.Min(blkBuffer.Length, .bytecount - BufferOffset)
                        End While
                    End With
                Next
                Return STATUS_SUCCESS
            End With
        Catch ex As Exception
            Return STATUS_FILE_CORRUPT_ERROR
        End Try
    End Function

    Public Overrides Function GetFileInfo(FileNode As Object, FileDesc0 As Object,
                                          ByRef FileInfo As Fsp.Interop.FileInfo) As Integer
        Dim OperationId As String
        Dim fileDesc = DirectCast(FileDesc0, FileDesc)
        If Not fileDesc Is Nothing Then
            If Not fileDesc.OperationId Is Nothing Then
                OperationId = fileDesc.OperationId
            End If
            Dim result As Integer = fileDesc.GetFileInfo(FileInfo)
            Return result
        End If
        '        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} 【GetFileInfo()】 OperationId:{OperationId}   FileNode:{JsonConvert.SerializeObject(FileNode)} FileDesc:{JsonConvert.SerializeObject(FileDesc)}")
        Return STATUS_NOT_FOUND
    End Function

    Public Overrides Function GetReparsePointByName(FileName As String, IsDirectory As Boolean,
                                                    ByRef ReparseData As Byte()) As Integer
        Console.WriteLine($"GetReparsePointByName FileName: {FileName} IsDirectory: {IsDirectory}")
        '        Return STATUS_NOT_A_REPARSE_POINT
        Return STATUS_INVALID_DEVICE_REQUEST
    End Function

    Public Overrides Function ReadDirectory(FileNode As Object, FileDesc0 As Object, Pattern As String, Marker As String,
                                            Buffer As IntPtr, Length As UInteger,
                                            <Out> ByRef BytesTransferred As UInteger) As Integer
        Dim OperationId As String
        Dim FileDesc = DirectCast(FileDesc0, FileDesc)
        If Not FileDesc Is Nothing Then
            If Not FileDesc.OperationId Is Nothing Then
                OperationId = FileDesc.OperationId
            End If
        End If
        '        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}  ReadDirectory() FileNode：{JsonConvert.SerializeObject(FileNode)} FileDesc0:{JsonConvert.SerializeObject(FileDesc0)} Pattern:{Pattern} Marker:{Marker}")

        Return MyBase.ReadDirectory(FileNode, FileDesc, Pattern, Marker, Buffer, Length, BytesTransferred)
    End Function

    Public Overrides Function ReadDirectoryEntry(FileNode As Object, FileDesc0 As Object, Pattern As String,
                                                 Marker As String, ByRef Context As Object,
                                                 <Out> ByRef FileName As String,
                                                 <Out> ByRef FileInfo As Fsp.Interop.FileInfo) As Boolean
        Dim OperationId As String
        Dim FileDesc = DirectCast(FileDesc0, FileDesc)
        If Not FileDesc Is Nothing Then
            If Not FileDesc.OperationId Is Nothing Then
                OperationId = FileDesc.OperationId
            End If
        End If
        '        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}  ReadDirectoryEntry() FileNode：{JsonConvert.SerializeObject(FileNode)} FileDesc0:{JsonConvert.SerializeObject(FileDesc0)} Pattern:{Pattern} Marker:{Marker}")

        If FileDesc Is Nothing OrElse FileDesc.FileSystemInfos Is Nothing Then
            If Pattern IsNot Nothing Then
                Pattern = Pattern.Replace("<", "").Replace(">", "").Replace("""", "")
            Else
                Pattern = ""
            End If
            '            Dim lst As New SortedList()
            '            If FileDesc.LTFSDirectory IsNot Nothing AndAlso FileDesc.Parent IsNot Nothing Then
            '                lst.Add(".", FileDesc.LTFSDirectory)
            '                lst.Add("..", FileDesc.Parent)
            '            End If
            Dim fileAndDir As SortedList
            If Pattern = "*" OrElse Pattern = "" Then
                fileAndDir = DirProvider.ReadDirWithWhere($"ParentPath= '{FileDesc.LTFSDirectory.fullpath}'", LW.GetSqliteConnection(LW.Barcode))
            Else
                fileAndDir = DirProvider.ReadDirWithWhere($"FullPath= '{FileDesc.LTFSDirectory.fullpath}\{Pattern}'", LW.GetSqliteConnection(LW.Barcode))
            End If
            ReDim FileDesc.FileSystemInfos(fileAndDir.Count - 1)
            fileAndDir.CopyTo(FileDesc.FileSystemInfos, 0)
        End If
        Dim index As Long = 0
        If Context Is Nothing Then
            If Marker IsNot Nothing Then
                index = Array.BinarySearch(FileDesc.FileSystemInfos, New DictionaryEntry(Marker, Nothing),
                                           _DirectoryEntryComparer)
                If index >= 0 Then
                    index += 1
                Else
                    index = -index
                End If
            End If
        Else
            index = CLng(Context)
        End If
        If FileDesc.FileSystemInfos.Length > index Then
            Context = index + 1
            FileName = FileDesc.FileSystemInfos(index).Key
            FileInfo = New Fsp.Interop.FileInfo()
            With FileDesc.FileSystemInfos(index)
                If TypeOf FileDesc.FileSystemInfos(index).Value Is ltfsindex.directory Then
                    With CType(FileDesc.FileSystemInfos(index).Value, ltfsindex.directory)
                        FileInfo.FileAttributes = FileDesc.dwFilAttributesValue.FILE_ATTRIBUTE_OFFLINE Or
                                                  FileDesc.dwFilAttributesValue.FILE_ATTRIBUTE_DIRECTORY
                        FileInfo.ReparseTag = 0
                        FileInfo.FileSize = 0
                        FileInfo.AllocationSize = 0
                        FileInfo.CreationTime = TapeUtils.ParseTimeStamp(.creationtime).ToFileTimeUtc
                        FileInfo.LastAccessTime = TapeUtils.ParseTimeStamp(.accesstime).ToFileTimeUtc
                        FileInfo.LastWriteTime = TapeUtils.ParseTimeStamp(.changetime).ToFileTimeUtc
                        FileInfo.ChangeTime = TapeUtils.ParseTimeStamp(.changetime).ToFileTimeUtc
                        FileInfo.IndexNumber = 0
                        FileInfo.HardLinks = 0
                    End With
                ElseIf TypeOf FileDesc.FileSystemInfos(index).Value Is ltfsindex.file Then
                    Dim f As ltfsindex.file = CType(FileDesc.FileSystemInfos(index).Value, ltfsindex.file)
                    With f
                        FileInfo.FileAttributes = FileDesc.dwFilAttributesValue.FILE_ATTRIBUTE_OFFLINE Or
                                                  FileDesc.dwFilAttributesValue.FILE_ATTRIBUTE_ARCHIVE
                        If .readonly Then _
                            FileInfo.FileAttributes = FileInfo.FileAttributes Or
                                                      FileDesc.dwFilAttributesValue.FILE_ATTRIBUTE_READONLY
                        FileInfo.ReparseTag = 0
                        FileInfo.FileSize = .length
                        FileInfo.CreationTime = TapeUtils.ParseTimeStamp(.creationtime).ToFileTimeUtc
                        FileInfo.LastAccessTime = TapeUtils.ParseTimeStamp(.accesstime).ToFileTimeUtc
                        FileInfo.LastWriteTime = TapeUtils.ParseTimeStamp(.changetime).ToFileTimeUtc
                        FileInfo.ChangeTime = TapeUtils.ParseTimeStamp(.changetime).ToFileTimeUtc
                        FileInfo.IndexNumber = 0
                        FileInfo.HardLinks = 0
                    End With
                End If
            End With
            Return True
        Else
            FileName = ""
            FileInfo = New Fsp.Interop.FileInfo()
            Return False
        End If
    End Function

    '        Public Overrides Function OverwriteEx(FileNode As Object, FileDesc As Object, FileAttributes As UInteger, ReplaceFileAttributes As Boolean, AllocationSize As ULong, Ea As IntPtr, EaLength As UInteger, ByRef fileInfo As Fsp.Interop.FileInfo) As Integer
    '    fileInfo = Nothing
    '    Dim fileDescObj As FileDesc = DirectCast(FileDesc, FileDesc)
    '    Dim fileDesc_FileName As String = "null"
    ''    
    '    If fileDescObj IsNot Nothing Then
    '        fileDesc_FileName = fileDescObj.LTFSFile.fullpath
    '        Console.WriteLine($"OverwriteEx {fileDesc_FileName} ")
    '        
    ''        If FileInfoDic.TryGetValue(fileDesc_FileName, fileInfo) Then
    ''            Return STATUS_SUCCESS
    ''        End If
    '    End If
    '    
    '    Console.WriteLine($"OverwriteEx {fileDesc_FileName} ")
    '    Return STATUS_ABANDONED
    'End Function
    '
    '    Public Overrides Function Write(fileNode As Object, fileDesc0 As Object, buffer As IntPtr, offset As ULong, length As UInteger, writeToEndOfFile As Boolean, constrainedIo As Boolean, ByRef pBytesTransferred As UInteger, ByRef fileInfo As Fsp.Interop.FileInfo) As Integer
    '        fileInfo = Nothing
    '        Dim fileDescObj As FileDesc = DirectCast(fileDesc0, FileDesc)
    '        pBytesTransferred = 0
    '        If fileDescObj IsNot Nothing Then
    '            fileDesc_FileName = fileDescObj.LTFSFile.fullpath
    '        End If
    '        Console.WriteLine($"Write {fileDesc_FileName} offset: {offset} length: {length} writeToEndOfFile: {writeToEndOfFile} constrainedIo: {constrainedIo}")
    '        
    '        Try
    '            Dim bytes1(length - 1) As Byte
    '            Marshal.Copy(buffer, bytes1, 0, bytes1.Length)
    '            pBytesTransferred = CUInt(bytes1.Length)
    '            fileInfo = New Fsp.Interop.FileInfo() With {
    '                .FileAttributes = CUInt(System.IO.FileAttributes.Archive),
    '                .AllocationSize = fileDescObj.LTFSFile.length,
    '                .FileSize = CULng(36626432),
    '                .CreationTime = 133545242966238200,
    '                .LastAccessTime = 133545242966238200,
    '                .LastWriteTime = 133545243282150536,
    '                .ChangeTime = 133545243282150536,
    '                .IndexNumber = 0,
    '                .HardLinks = 0,
    '                .EaSize = 0
    '            }
    '            
    '            Return STATUS_SUCCESS
    '        Catch ex As Exception
    '            Console.WriteLine($"Exception {ex.Message} {ex.StackTrace}")
    '            ' Handle exceptions here
    '        End Try
    '    End Function
    Public Overrides Function Create(fileName As String, createOptions As UInteger, grantedAccess As UInteger,
                                     fileAttributes As UInteger, securityDescriptor As Byte(), allocationSize As ULong,
                                     ByRef fileNode As Object, ByRef fileDesc0 As Object,
                                     ByRef fileInfo As Fsp.Interop.FileInfo, ByRef normalizedName As String) As Integer
        '        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}  【Create】 {fileName} createOptions {createOptions} grantedAccess {grantedAccess} fileAttributes {fileAttributes} allocationSize {allocationSize}")
        fileNode = Nothing
        normalizedName = Nothing
        fileInfo = Nothing
        fileDesc0 = Nothing
        normalizedName = ""
        Dim fileDesc As FileDesc
        If LW.schema._directory.Count = 0 Then Throw New Exception("Not LTFS formatted")
        Dim path As String() = fileName.Split({"\"}, StringSplitOptions.RemoveEmptyEntries)
        If path.Length = 0 Then
            Return STATUS_NOT_FOUND
        End If
        Dim FileExist As Boolean = False

        Dim LTFSDir = DirProvider.QueryDir(fileName, LW.GetSqliteConnection(LW.Barcode))
        If Not LTFSDir Is Nothing Then
            FileExist = True
            fileDesc = New FileDesc _
                With {.IsDirectory = True, .LTFSDirectory = LTFSDir, .OperationId = Guid.NewGuid().ToString()}
            Console.WriteLine($"文件夹名字冲突：{LTFSDir.name}下的{LTFSDir.name}")
            Return STATUS_OBJECT_NAME_COLLISION
        Else
            Dim LTFSFile = DirProvider.QueryFile(fileName, LW.GetSqliteConnection(LW.Barcode))
            If Not LTFSFile Is Nothing Then
                FileExist = True
                fileDesc = New FileDesc _
                    With {.IsDirectory = False, .LTFSFile = LTFSFile,
                        .OperationId = Guid.NewGuid().ToString()}
                Console.WriteLine($"文件名字冲突：{LTFSDir.name}下的{LTFSFile.name}")
                Return STATUS_OBJECT_NAME_COLLISION
            End If
        End If

        Try
            ' directory or file?
            If (createOptions And FILE_DIRECTORY_FILE) = 0 Then
                ' file

                Dim Security As FileSecurity = Nothing

                If securityDescriptor IsNot Nothing Then
                    Security = New FileSecurity()
                    Security.SetSecurityDescriptorBinaryForm(securityDescriptor)
                End If
                Dim fileRecord = New LTFSWriter.FileRecord(path(path.Length - 1), allocationSize, fileName, LTFSDir)
                '                fileDesc = New FileDescriptor(fileName, path, New MemoryStream(), True)
                '                fileDesc.SetFileAttributes(fileAttributes Or CUInt(System.IO.FileAttributes.Archive))


                fileInfo = New Fsp.Interop.FileInfo() With {
                    .FileAttributes = CUInt(System.IO.FileAttributes.Archive),
                    .AllocationSize = CULng(0),
                    .FileSize = CULng(0),
                    .CreationTime = Now.ToFileTimeUtc,
                    .LastAccessTime = .CreationTime,
                    .LastWriteTime = .CreationTime,
                    .ChangeTime = .CreationTime,
                    .IndexNumber = 0,
                    .HardLinks = 0,
                    .EaSize = 0
                    }
                '                fileDesc0 = new FileDesc(fileRecord) fileRecord
                fileDesc0 = New FileDesc _
                    With {.IsDirectory = False, .UnwriteFile = fileRecord, .LTFSFile = fileRecord.File,
                        .Parent = LTFSDir, .OperationId = Guid.NewGuid().ToString()}
                fileRecord.File.fullpath = fileName
                '                filedesc.GetFileInfo(FileInfo)
                '                Console.WriteLine(($"New FileDesc Create：{LTFSDir.name}下的{fileName} {fileDesc0.OperationId}"))
            Else

                Dim newdir As New ltfsindex.directory With {
                        .name = path(path.Length - 1),
                        .creationtime = Now.ToUniversalTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z"),
                        .fileuid = Me.LW.schema.highestfileuid + 1,
                        .backuptime = .creationtime,
                        .accesstime = .creationtime,
                        .changetime = .creationtime,
                        .modifytime = .creationtime,
                        .readonly = False
                        }
                Me.LW.schema.highestfileuid += 1
                '                LTFSDir.contents._directory.Add(newdir)
                DirProvider.InsertDir(newdir, IO.Path.GetDirectoryName(fileName), LW.GetSqliteConnection(LW.Barcode),LW.Barcode)
                fileDesc0 = New FileDesc _
                    With {.IsDirectory = True, .LTFSDirectory = newdir, .OperationId = Guid.NewGuid().ToString()}
                '                Console.WriteLine(($"New FileDesc Create：{LTFSDir.name}下的{newdir.name} {fileDesc0.OperationId}"))
                fileInfo = New Fsp.Interop.FileInfo() With {
                    .FileAttributes = CUInt(System.IO.FileAttributes.Directory),
                    .AllocationSize = CULng(0),
                    .FileSize = CULng(0),
                    .CreationTime = TapeUtils.ParseTimeStamp(newdir.creationtime).ToFileTimeUtc,
                    .LastAccessTime = .CreationTime,
                    .LastWriteTime = .CreationTime,
                    .ChangeTime = .CreationTime,
                    .IndexNumber = 0,
                    .HardLinks = 0,
                    .EaSize = 0
                    }

                Return STATUS_SUCCESS
                ' directory
            End If

            Return STATUS_SUCCESS
        Catch ex As Exception
            Console.WriteLine(ex.Message & ex.StackTrace)
        Finally
        End Try

        Return STATUS_ABANDONED
    End Function

    Public Overrides Function SetBasicInfo(fileNode As Object, fileDesc0 As Object, fileAttributes As UInteger,
                                           creationTime As ULong, lastAccessTime As ULong, lastWriteTime As ULong,
                                           changeTime As ULong, ByRef fileInfo As Fsp.Interop.FileInfo) As Integer
        Dim fileDesc As FileDesc = DirectCast(fileDesc0, FileDesc)

        Dim OperationId As String
        If Not fileDesc Is Nothing Then
            If Not fileDesc.OperationId Is Nothing Then
                OperationId = fileDesc.OperationId
            End If
        Else
            '            Console.WriteLine(
            '                $"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} 【SetBasicInfo】  OperationId:{OperationId _
            '                                 }  fileDesc_FileName: OperationId:{OperationId} fileAttributes:{fileAttributes _
            '                                 } creationTime:{creationTime} lastAccessTime:{lastAccessTime} lastWriteTime:{ _
            '                                 lastWriteTime} changeTime:{changeTime}")
            Return STATUS_NOT_FOUND
        End If
        Dim fileDesc_FileName As String = "null"


        If fileDesc IsNot Nothing Then
            If fileDesc.IsDirectory Then
                fileDesc_FileName = fileDesc.LTFSDirectory.name
                If creationTime > 0 Then
                    fileDesc.LTFSDirectory.creationtime =
                        DateTimeOffset.FromFileTime(creationTime).ToUniversalTime.ToString(
                            "yyyy-MM-ddTHH:mm:ss.fffffff00Z")
                End If
                If lastWriteTime > 0 Then
                    fileDesc.LTFSDirectory.modifytime =
                        DateTimeOffset.FromFileTime(lastWriteTime).ToUniversalTime.ToString(
                            "yyyy-MM-ddTHH:mm:ss.fffffff00Z")
                    fileDesc.LTFSDirectory.changetime = fileDesc.LTFSDirectory.modifytime
                End If
                If lastAccessTime > 0 Then
                    fileDesc.LTFSDirectory.accesstime =
                        DateTimeOffset.FromFileTime(lastAccessTime).ToUniversalTime.ToString(
                            "yyyy-MM-ddTHH:mm:ss.fffffff00Z")
                End If
                Dim updateInfo = New ltfsindex.file With {
                        .fullpath = fileDesc.LTFSDirectory.fullpath,
                        .creationtime = fileDesc.LTFSDirectory.creationtime,
                        .accesstime = fileDesc.LTFSDirectory.accesstime,
                        .changetime = fileDesc.LTFSDirectory.changetime,
                        .modifytime = fileDesc.LTFSDirectory.modifytime
                }
                DirProvider.UpdateBaseInfo(updateInfo, LW.GetSqliteConnection(LW.Barcode),LW.Barcode)
            Else
                fileDesc_FileName = fileDesc.LTFSFile.name
                ' MakeWriteable(fileDesc)
                If creationTime > 0 Then
                    fileDesc.LTFSFile.creationtime =
                        DateTimeOffset.FromFileTime(creationTime).ToUniversalTime.ToString(
                            "yyyy-MM-ddTHH:mm:ss.fffffff00Z")
                End If
                If lastWriteTime > 0 Then
                    fileDesc.LTFSFile.modifytime =
                        DateTimeOffset.FromFileTime(lastWriteTime).ToUniversalTime.ToString(
                            "yyyy-MM-ddTHH:mm:ss.fffffff00Z")
                    fileDesc.LTFSFile.changetime = fileDesc.LTFSFile.modifytime
                End If
                If lastAccessTime > 0 Then
                    fileDesc.LTFSFile.accesstime =
                        DateTimeOffset.FromFileTime(lastAccessTime).ToUniversalTime.ToString(
                            "yyyy-MM-ddTHH:mm:ss.fffffff00Z")
                End If
                DirProvider.UpdateBaseInfo(fileDesc.LTFSFile, LW.GetSqliteConnection(LW.Barcode),LW.Barcode)
            End If

        End If

        '        Console.WriteLine(
        '            $"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} 【SetBasicInfo】  OperationId:{OperationId _
        '                             }  fileDesc_FileName:{fileDesc_FileName} OperationId:{OperationId} fileAttributes:{ _
        '                             fileAttributes} creationTime:{creationTime} lastAccessTime:{lastAccessTime} lastWriteTime:{ _
        '                             lastWriteTime} changeTime:{changeTime}")


        Return fileDesc.GetFileInfo(fileInfo)
    End Function

    Public Overrides Function SetFileSize(fileNode As Object, fileDesc0 As Object, newSize As ULong,
                                          setAllocationSize As Boolean, ByRef fileInfo As Fsp.Interop.FileInfo) _
        As Integer

        Dim fileDesc As FileDesc = DirectCast(fileDesc0, FileDesc)
        Dim OperationId As String
        If Not fileDesc Is Nothing Then
            If Not fileDesc.OperationId Is Nothing Then
                OperationId = fileDesc.OperationId
            End If
        End If
        Dim fileDesc_FileName As String = "null"

        If fileDesc IsNot Nothing Then
            fileDesc_FileName = fileDesc.LTFSFile.name
        End If

        '        Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}   【SetFileSize】  fileDesc_FileName:{fileDesc_FileName} OperationId:{OperationId} size:{newSize} allocationSize: {setAllocationSize} ")
        ' MakeWriteable(fileDesc)
        If Not setAllocationSize Then
            fileDesc.LTFSFile.length = newSize
            DirProvider.UpdateFileSize(fileDesc.LTFSFile, LW.GetSqliteConnection(LW.Barcode),LW.Barcode)
        End If
        Return fileDesc.GetFileInfo(fileInfo)
    End Function


    Private Shared lockObject As New Object()

    '记录上一次文件总大小，用于每隔300MB读取一个PositionData
    Public Shared LastNewPositionSize As Long
    '记录上一次文件数量，用于每隔300个文件读取一个PositionData
    Public Shared LastNewPositionFileCount As Long
    Public Shared WriteFileQueue As New ConcurrentQueue(Of FileDesc)

    Public Sub StartRefreshThread()
        Dim refreshThread As New Thread(AddressOf RefreshDisplayThread)
        refreshThread.IsBackground = True
        refreshThread.Start()
    End Sub

    Private Sub RefreshDisplayThread()
        While True
            Dim count As Integer = 0
            Dim startTime As DateTime = DateTime.Now

            ' 从队列中取出最多1000个元素，并执行refresh方法
            While count < 1000
                Dim fileDesc As FileDesc = Nothing
                If WriteFileQueue.TryDequeue(fileDesc) Then
                    ' 执行refresh方法
                    count += 1
                Else
                    ' 检查是否超过1秒
                    Dim elapsedTime As TimeSpan = DateTime.Now - startTime
                    If elapsedTime.TotalSeconds >= 5 Then
                        Exit While
                    End If
                End If
            End While
            If count > 0 Then
                ' LW.RefreshDisplay()
            End If
        End While
    End Sub

    Private Shared HashTaskAwaitNumber As Integer = 0

    Public Overrides Function Write(fileNode As Object, fileDesc0 As Object, buffer As IntPtr, offset As ULong,
                                    length As UInteger, writeToEndOfFile As Boolean, constrainedIo As Boolean,
                                    ByRef pBytesTransferred As UInteger, ByRef fileInfo As Fsp.Interop.FileInfo) _
        As Integer
        fileInfo = Nothing
        Dim FileDesc = DirectCast(fileDesc0, FileDesc)
        Dim OperationId As String
        If Not FileDesc Is Nothing Then
            If Not FileDesc.OperationId Is Nothing Then
                OperationId = FileDesc.OperationId
            End If
        End If
        Dim fileDesc_FileName As String = "null"
        pBytesTransferred = 0
        If FileDesc IsNot Nothing Then
            fileDesc_FileName = FileDesc.LTFSFile.name
        End If
        Threading.ThreadPool.SetMaxThreads(256, 256)
        Threading.ThreadPool.SetMinThreads(128, 128)
        SyncLock lockObject

            Try
                Dim p As TapeUtils.PositionData
                If FileDesc.Position Is Nothing Then
                    If LW.WinFspPositionData Is Nothing OrElse
                       LW.TotalFilesProcessed - LastNewPositionFileCount > 1000 OrElse
                       LW.TotalBytesProcessed - LastNewPositionSize > 300L * 1024 * 1024 Then
'                        TapeUtils.Locate(TapeDrive, LW.CurrentHeight, LW.DataPartition)
                        LW.WinFspPositionData = New TapeUtils.PositionData(TapeDrive)
                        LastNewPositionSize = LW.TotalBytesProcessed
                        LastNewPositionFileCount = LW.TotalFilesProcessed
                        LW.PrintMsg($"Position = {LW.WinFspPositionData.ToString()}", LogOnly:=True)
                        '                        Console.WriteLine(
                        '                            $"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}   【PositionData】 JSON { _
                        '                                             JsonConvert.SerializeObject(PositionData)} ")

                    End If
                    p = LW.WinFspPositionData 'new TapeUtils.PositionData(TapeDrive)
                    FileDesc.Position = p
                Else
                    p = FileDesc.Position
                End If
                If p.BlockNumber < LW.CurrentHeight Then
                    Metric.FuncFileOperationDuration(Sub()
                       TapeUtils.Locate(TapeDrive, LW.CurrentHeight, LW.DataPartition)
                    End Sub,  {"", "TapeUtils_Write_Locate", ""})
                    p.BlockNumber = LW.CurrentHeight
                    End

                End If
                If offset > FileDesc.LTFSFile.WriteedOffset + FileDesc.LTFSFile.length Then
                    LW.PrintMsg($"winfsp Write 错误,只支持单线程顺序写入, {fileDesc_FileName}:offset{offset} length:{length}  WriteedOffset:{FileDesc.LTFSFile.WriteedOffset} FileDesc.LTFSFile.Length:{FileDesc.LTFSFile.length} ", ForceLog:=True, Warning:=True)
                    Return STATUS_FT_WRITE_FAILURE
                End If
                'todo : 不是create操作的write，返回失败，暂不支持
                If offset = 0 Then
                    '新的文件
                    Dim fileextent As New ltfsindex.file.extent With
                            {.partition = ltfsindex.PartitionLabel.b,
                            .startblock = p.BlockNumber,
                            .bytecount = FileDesc.LTFSFile.length,
                            .byteoffset = 0,
                            .fileoffset = 0}

                    FileDesc.LTFSFile.extentinfo.Clear()
                    FileDesc.LTFSFile.extentinfo.Add(fileextent)
                    '                TapeUtils.SetBlockSize(TapeDrive, LW.plabel.blocksize)
                    Metric.OperationCounter.WithLabels("hashfile").Inc()
                    '                    If LW.HashOnWrite Then 
                    FileDesc.Sh = New IOManager.CheckSumBlockwiseCalculator
                    '                    End If
                End If

                Dim len As Integer = length
                Dim Barcode = LW.Barcode
                Dim plabel = LW.plabel

                Dim sense As Byte()
                '            Dim writeBuffer(plabel.blocksize - 1) As Byte

                '            Marshal.Copy(writeBuffer, 0, buffer, writeBuffer.Length)
                Dim writeOffset As Integer = 0
                Do While writeOffset < len
                    Dim succ = False
                    Dim writelength As Integer = Math.Min(plabel.blocksize, len - writeOffset)

                    '                Marshal.Copy(buffer,  writeBuffer,0, writelength)
                    ' 在此处处理缓冲区中的数据，比如输出到控制台或者写入文件等操作
                    While Not succ
                        Try
                            Dim startTimestamp = DateTime.Now

                            sense = TapeUtils.Write(TapeDrive, buffer + writeOffset, writelength)
                            Dim duration As TimeSpan = DateTime.Now - startTimestamp
                            Metric.FileOperationDurationHistogram.WithLabels(Barcode, "Tape_Write", "").Observe(duration.TotalMilliseconds)
                            Metric.FileOperationDurationSummary.WithLabels(Barcode, "Tape_Write", "").Observe(duration.TotalMilliseconds)

                            writeOffset += writelength
                            '                        sense=TapeUtils.Write(TapeDrive, writeBuffer, writelength)
                        Catch ex As Exception

                            LW.PrintMsg($"Current Position = {p.ToString()} {ex.Message} {ex.StackTrace}",
                                        LogOnly:=True)

                            Select Case _
                                MessageBox.Show(My.Resources.ResText_WErrSCSI, My.Resources.ResText_Warning,
                                                MessageBoxButtons.AbortRetryIgnore)
                                Case DialogResult.Abort
                                    Throw ex
                                Case DialogResult.Retry
                                    succ = False
                                Case DialogResult.Ignore
                                    succ = True
                                    Exit While
                            End Select
                            p = New TapeUtils.PositionData(TapeDrive)
                            LW.PrintMsg($"New Position = {p.ToString()} {ex.Message} {ex.StackTrace}", LogOnly:=True)
                            Continue While
                        End Try
                        Metric.OperationCounter.WithLabels(("write_file")).Inc(len)
                        If ((sense(2) >> 6) And &H1) = 1 Then
                            If (sense(2) And &HF) = 13 Then
                                '磁带已满                                           
                                Console.WriteLine("磁带已满")
                                LW.StopFlag = True
                            Else
                                '磁带即将写满
                                Console.WriteLine("磁带即将写满")
                                succ = True
                            End If
                        ElseIf sense(2) And &HF <> 0 Then
                            '写入出错
                            Console.WriteLine("写入出错")
                        Else
                            succ = True
                        End If
                        If succ Then
                            Dim file = FileDesc.LTFSFile

                        End If
                    End While
                    '                writeOffset += writelength
                    '                succ = True
                    If succ Then
                        pBytesTransferred += Convert.ToInt64(writelength)
                        FileDesc.LTFSFile.WrittenBytes += writelength
                        LW.TotalBytesProcessed += writelength
                        LW.CurrentBytesProcessed += writelength
                        LW._TotalBytesUnindexed += writelength
                        FileDesc.LTFSFile.length = offset + length
                        FileDesc.LTFSFile.extentinfo(0).bytecount = FileDesc.LTFSFile.length
                        FileDesc.LTFSFile.WriteedOffset = offset + length
                        SyncLock p
                            p.BlockNumber += 1
                            LW.CurrentHeight = p.BlockNumber
                            DirProvider.UpdateCurrentHeight(LW.GetSqliteConnection(LW.Barcode), LW.Barcode, LW.CurrentHeight,LW._TotalBytesUnindexed)
                        End SyncLock
                        If FileDesc.Sh IsNot Nothing Then
                            Dim startTimestamp1 = DateTime.Now

                            Dim writeBuffer(writelength - 1) As Byte
                            Dim duration1 As TimeSpan = DateTime.Now - startTimestamp1
                            Metric.FileOperationDurationHistogram.WithLabels(Barcode, "new_writeBuffer", "").Observe(duration1.TotalMilliseconds)
                            Metric.FileOperationDurationSummary.WithLabels(Barcode, "new_writeBuffer", "").Observe(duration1.TotalMilliseconds)

                            Metric.FuncFileOperationDuration(Sub()
                                                                 Marshal.Copy(buffer + writeOffset - writelength, writeBuffer, 0, writeBuffer.Length)
                                                             End Sub, {Barcode, "Marshal_Copy_writeBuffer", ""})


                            Metric.OperationCounter.WithLabels("hashfile_block").Inc()
                            '                            If LW.异步校验CPU占用高ToolStripMenuItem.Checked Then
                            Metric.FuncFileOperationDuration(Sub()
                                                                 FileDesc.Sh.PropagateAsync(writeBuffer, writeBuffer.Length)
                                                             End Sub, {Barcode, "async_sha", ""})
                            '                            Else
                            '                                FileDesc.Sh.Propagate(writeBuffer, writeBuffer.length)
                            '                                Dim duration2 As TimeSpan = DateTime.Now - startTimestamp2
                            '                                Metric.FileOperationDurationHistogram.WithLabels(Barcode, "sync_sha", "").Observe(duration2.TotalMilliseconds)
                            '                                Metric.FileOperationDurationSummary.WithLabels(Barcode, "sync_sha", "").Observe(duration2.TotalMilliseconds)
                            '                            End If
                        End If
                    End If
                    If Not succ Then
                        LW.PrintMsg(($"winfsp Write not succ {fileDesc_FileName} {offset} {length} {writeOffset} {writelength}"))
                        Return STATUS_FT_WRITE_FAILURE
                    End If

                Loop


                '      FileDesc.LTFSFile.length=FileDesc.LTFSFile.length+length
                Return FileDesc.GetFileInfo(fileInfo)

            Catch ex As Exception
                LW.PrintMsg($"Exception {ex.Message} {ex.StackTrace}")
                Console.WriteLine($"Exception {ex.Message} {ex.StackTrace}")
                Return STATUS_ABANDONED
            Finally
                Dim tempFileInfo = fileInfo
                '            Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} 【Writed】   fileDesc_FileName:{fileDesc_FileName} PBytesTransferred:{pBytesTransferred} fileinfo:{JsonConvert.SerializeObject(tempFileInfo)}")
            End Try
        End SyncLock
    End Function

    Public Overrides Function OverwriteEx(ByVal fileNode As Object, ByVal fileDesc0 As Object,
                                          ByVal FileAttributes As UInt32, ByVal ReplaceFileAttributes As Boolean,
                                          ByVal AllocationSize As UInt64, ByVal Ea As IntPtr, ByVal EaLength As UInt32,
                                          ByRef FileInfo As Fsp.Interop.FileInfo) As Int32

        FileInfo = Nothing
        Dim fileDesc = DirectCast(fileDesc0, FileDesc)
        Dim OperationId As String
        If Not fileDesc Is Nothing Then
            If Not fileDesc.OperationId Is Nothing Then
                OperationId = fileDesc.OperationId
            End If
        End If
        Dim fileDesc_FileName As String = "null"

        If fileDesc IsNot Nothing Then
            fileDesc_FileName = fileDesc.LTFSFile.name
        End If

        DirProvider.DeleteFile(fileDesc.LTFSFile, LW.GetSqliteConnection(LW.Barcode), LW.Barcode)
        Dim fileRecord = New LTFSWriter.FileRecord(fileDesc.LTFSFile.name, 0, "", Nothing)
        '                fileDesc = New FileDescriptor(fileName, path, New MemoryStream(), True)
        '                fileDesc.SetFileAttributes(fileAttributes Or CUInt(System.IO.FileAttributes.Archive))

        fileDesc.LTFSFile = fileRecord.File
        fileDesc.LTFSFile.accesstime = Now.ToUniversalTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")
        fileDesc.LTFSFile.changetime = fileDesc.LTFSFile.accesstime
        fileDesc.LTFSFile.creationtime = fileDesc.LTFSFile.accesstime
        fileDesc.LTFSFile.modifytime = fileDesc.LTFSFile.accesstime
        fileDesc.LTFSFile.sha1 = ""
        fileDesc.LTFSFile.length = 0


        '    

        fileDesc.UnwriteFile = fileRecord
        fileDesc0 = New FileDesc _
            With {.IsDirectory = False, .UnwriteFile = fileRecord, .LTFSFile = fileRecord.File,
                .LTFSDirectory = fileDesc.Parent, .OperationId = Guid.NewGuid().ToString()}


        Return fileDesc.GetFileInfo(FileInfo)
    End Function

    Public Overrides Sub Cleanup(ByVal fileNode As Object, ByVal fileDesc0 As Object, ByVal fileName As String,
                                 ByVal flags As UInt32)

        Dim fileDesc = DirectCast(fileDesc0, FileDesc)
        Dim OperationId As String
        If Not fileDesc Is Nothing Then
            If Not fileDesc.OperationId Is Nothing Then
                OperationId = fileDesc.OperationId
            End If
        End If
        Dim fileDesc_FileName As String = "null"

        If fileDesc IsNot Nothing Then
            If fileDesc.IsDirectory Then
                fileDesc_FileName = fileDesc.LTFSDirectory.name
            Else
                fileDesc_FileName = fileDesc.LTFSFile.name
            End If

        End If

        '    ' 如果Flags包含CleanupSetArchiveBit标志，并且主文件节点不是目录，则设置主文件节点的Archive属性为1
        '    If (Flags And CleanupSetArchiveBit) <> 0 Then
        '       
        '    End If

        ' 如果Flags包含CleanupSetLastAccessTime、CleanupSetLastWriteTime或CleanupSetChangeTime标志
        If (flags And (CleanupSetLastAccessTime Or CleanupSetLastWriteTime Or CleanupSetChangeTime)) <> 0 Then
            ' 获取当前系统时间的文件时间表示形式（以100纳秒为单位）
            Dim SystemTime = Now.ToUniversalTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffff00Z")

            ' 根据Flags设置主文件节点的最后访问时间、最后写入时间和修改时间
            '            If (Flags And CleanupSetLastAccessTime) <> 0 Then
            '                if fileDesc.IsDirectory Then
            '                    fileDesc.LTFSDirectory.accesstime = SystemTime
            '                Else
            '                    fileDesc.LTFSFile.accesstime = SystemTime
            '                End If
            '
            '            End If
            '            If (Flags And CleanupSetLastWriteTime) <> 0 Then
            '                if fileDesc.IsDirectory Then
            '                    fileDesc.LTFSDirectory.modifytime = SystemTime
            '                Else
            '                    fileDesc.LTFSFile.modifytime = SystemTime
            '                End If
            '
            '            End If
            '            If (Flags And CleanupSetChangeTime) <> 0 Then
            '                if fileDesc.IsDirectory Then
            '                    fileDesc.LTFSDirectory.changetime = SystemTime
            '                Else
            '                    fileDesc.LTFSFile.changetime = SystemTime
            '                End If
            '            End If
        End If

        '    ' 如果Flags包含CleanupSetAllocationSize标志
        '    If (Flags And CleanupSetAllocationSize) <> 0 Then
        '        ' 根据分配单元的大小计算新的分配大小
        '       
        '    End If

        ' 如果Flags包含CleanupDelete标志，并且FileNode没有子节点
        If (flags And CleanupDelete) <> 0 Then

        End If

        If Not fileDesc.UnwriteFile Is Nothing Then
            LW.TotalFilesProcessed += 1
            LW.CurrentFilesProcessed += 1
            '            FileDesc.Parent.contents._file.Add(FileDesc.LTFSFile)

            fileDesc.UnwriteFile = Nothing
            fileDesc.LTFSFile.fileuid = LW.schema.highestfileuid + 1
            LW.schema.highestfileuid += 1
            DirProvider.UpdateHightestFileUid(LW.GetSqliteConnection(LW.Barcode), LW.Barcode, LW.schema.highestfileuid)
            If fileDesc.Sh IsNot Nothing Then
                fileDesc.Sh.ProcessFinalBlock()
                Metric.OperationCounter.WithLabels("hashfile_finish").Inc()
                fileDesc.LTFSFile.SetXattr(ltfsindex.file.xattr.HashType.SHA1, fileDesc.Sh.SHA1Value)
                fileDesc.LTFSFile.SetXattr(ltfsindex.file.xattr.HashType.MD5, fileDesc.Sh.MD5Value)
                fileDesc.Sh.StopFlag = True
                DirProvider.InsertFile(fileDesc.LTFSFile, Path.GetDirectoryName(fileDesc.LTFSFile.fullpath), LW.GetSqliteConnection(LW.Barcode), LW.Barcode)

            End If
            WriteFileQueue.Enqueue(fileDesc)
            If LW.CheckUnindexedDataSizeLimit() Then LW.WinFspPositionData = New TapeUtils.PositionData(TapeDrive)
'            LW.Invoke(Sub()
'                If _
'                         LW.CapacityRefreshInterval > 0 AndAlso
'                         (Now - LW.LastRefresh).TotalSeconds > LW.CapacityRefreshInterval _
'                         Then
'                    PositionData = New TapeUtils.PositionData(TapeDrive)
'
'                    Dim p2 As New TapeUtils.PositionData(TapeDrive)
'                    If _
'                         p2.BlockNumber <> PositionData.BlockNumber OrElse
'                         p2.PartitionNumber <> PositionData.PartitionNumber _
'                         Then
'                        If _
'                         MessageBox.Show($"Position changed! {PositionData.BlockNumber} -> {p2.BlockNumber}", "Warning",
'                                         MessageBoxButtons.OKCancel) = DialogResult.Cancel Then
'                            LW.StopFlag = True
'                        End If
'                    End If
''                    LW.RefreshDisplay()
'                End If
'            End Sub)
        End If
    End Sub
End Class
