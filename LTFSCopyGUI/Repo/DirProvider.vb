Imports System.Collections.Concurrent
Imports System.Data.Common
Imports System.Data.SQLite
Imports System.IO
Imports System.Runtime.CompilerServices
Imports System.Web.UI.WebControls
Imports Newtonsoft.Json

'   Public Property name As String
'        Public Property length As Long
'        Public Property [readonly] As Boolean = False
'        Public Property openforwrite As Boolean = True
'        Public Property creationtime As String
'        Public Property changetime As String
'        Public Property modifytime As String
'        Public Property accesstime As String
'        Public Property backuptime As String
'        Public Property fileuid As Long

Public Class DirProvider
    Public Shared Function CreateConnection(Path As String) As SQLiteConnection
        Dim connectionString As String = $"Data Source={Path};Version=3;"
        Dim conn As New SQLiteConnection(connectionString)
        Return conn
    End Function

    Public Shared Sub CreateDatabaseAndTable(conn As SQLiteConnection)
        Dim versionNumber As Integer = 1 ' 初始版本号
        ' 如果数据库不存在，此操作将会创建它
        ' 因为SQLite会在首次连接时自动创建数据库文件
        Dim createVersionTableCommand As New SQLiteCommand(
            "CREATE TABLE IF NOT EXISTS version (
                number INTEGER
                );",
            conn
            )
        createVersionTableCommand.ExecuteNonQuery()

        ' 检查是否已经存在版本号记录
        Dim checkVersionCommand As New SQLiteCommand(
            "SELECT COUNT(*) FROM version;",
            conn
            )
        Dim existingRecords As Integer = Convert.ToInt32(checkVersionCommand.ExecuteScalar())

        ' 如果不存在版本号记录，则插入初始版本号
        If existingRecords = 0 Then
            Dim insertVersionCommand As New SQLiteCommand(
                "INSERT INTO version (number) VALUES (@number);",
                conn
                )
            insertVersionCommand.Parameters.AddWithValue("@number", versionNumber)
            insertVersionCommand.ExecuteNonQuery()
        End If
        ' 创建表
        Dim createTableCommand As New SQLiteCommand(
            "CREATE TABLE IF NOT EXISTS ltfs_index (
                ID INTEGER PRIMARY KEY,
                FullPath TEXT NOT NULL COLLATE NOCASE,
                ParentPath TEXT NOT NULL COLLATE NOCASE,
                Name TEXT NOT NULL COLLATE NOCASE,
                ReadOnly INTEGER ,
                OpenForWrite INTEGER ,
                IsDirectory INTEGER NOT NULL,
                Length INTEGER ,
                CreationTime TEXT COLLATE NOCASE,
                ChangeTime TEXT COLLATE NOCASE,
                ModifyTime TEXT COLLATE NOCASE,
                AccessTime TEXT COLLATE NOCASE,
                BackupTime Text COLLATE NOCASE,
                SHA1 TEXT COLLATE NOCASE,
                FileUID INTEGER ,
                StartBlock INTEGER ,
                extent TEXT COLLATE NOCASE,
                extendedattributes TEXT COLLATE NOCASE 
                );",
            conn
            )

        createTableCommand.ExecuteNonQuery() ' 执行SQL命令创建表

        ' 创建索引Path,ParentID,Length
        Dim createIndexCommand As New SQLiteCommand(
            "CREATE Unique INDEX IF NOT EXISTS PathIndex ON ltfs_index (FullPath);",
            conn
            )
        createIndexCommand.ExecuteNonQuery()
        ' 创建索引Path,ParentID,Length
        createIndexCommand = New SQLiteCommand(
            "CREATE INDEX IF NOT EXISTS ParentPathIndex ON ltfs_index (ParentPath);",
            conn
            )
        createIndexCommand.ExecuteNonQuery()
        createIndexCommand = New SQLiteCommand(
            "CREATE INDEX IF NOT EXISTS LengthIndex ON ltfs_index (Length);",
            conn
            )
        createIndexCommand.ExecuteNonQuery()
        createIndexCommand = New SQLiteCommand(
            "CREATE INDEX IF NOT EXISTS StartBlockIndex ON ltfs_index (StartBlock);",
            conn
            )
        createIndexCommand.ExecuteNonQuery()
        createIndexCommand = New SQLiteCommand(
            "CREATE INDEX IF NOT EXISTS FileUidIndex ON ltfs_index (FileUid);",
            conn
            )
        createIndexCommand.ExecuteNonQuery()
        createIndexCommand = New SQLiteCommand(
            "CREATE INDEX IF NOT EXISTS Sha1Index ON ltfs_index (Sha1);",
            conn
            )

        createIndexCommand.ExecuteNonQuery()
        Dim createInfoTableCommand As New SQLiteCommand(
         "CREATE TABLE IF NOT EXISTS ltfs_index_info (
                ID INTEGER PRIMARY KEY,
                creator TEXT COLLATE NOCASE,
                blocksize INTEGER ,
                volumeuuid TEXT  COLLATE NOCASE,
                formattime TEXT  COLLATE NOCASE,
                generationnumber   COLLATE NOCASE,
                updatetime TEXT COLLATE NOCASE,
                location_partition TEXT  COLLATE NOCASE,
                location_startblock INTEGER  ,
                prelocation_partition TEXT  COLLATE NOCASE,
                prelocation_startblock INTEGER  ,
                highestfileuid INTEGER,
                current_height INTEGER,
                TotalBytesProcessed INTEGER,
                CurrentBytesProcessed INTEGER,
                TotalBytesUnindexed INTEGER
                );",
         conn
         )
        createInfoTableCommand.ExecuteNonQuery()

    End Sub

    Public Shared Sub UpdateSchema(connection As SQLiteConnection, plabel As ltfslabel, schema As ltfsindex, currentHeight As Int64, BarCode As String)
        Dim ExistsCommand As New SQLiteCommand(
            "SELECT COUNT(*) FROM ltfs_index_info ",
            connection
            )
        Dim ExistsResult As Integer = CInt(ExistsCommand.ExecuteScalar())
        If ExistsResult > 0 Then

            Dim DelCommand As New SQLiteCommand(
            "Delete from FROM ltfs_index_info ",
            connection
            )
            Dim DelCommandResult As Integer = CInt(ExistsCommand.ExecuteScalar())
            InitializeLTFSIndexInfo(connection, plabel, schema, currentHeight, BarCode)

        ElseIf ExistsResult > 1 Then
            Throw New Exception("ltfs_index_info table has more than one record")
        End If
    End Sub
    Public Shared Sub InitializeLTFSIndexInfo(connection As SQLiteConnection, plabel As ltfslabel, schema As ltfsindex, currentHeight As Int64, BarCode As String)
        Dim ExistsCommand As New SQLiteCommand(
            "SELECT COUNT(*) FROM ltfs_index_info ",
            connection
            )
        Dim ExistsResult As Integer = CInt(ExistsCommand.ExecuteScalar())
        If ExistsResult = 0 Then
            Dim InsertCommand As New SQLiteCommand(
                "insert into ltfs_index_info (creator,blocksize,volumeuuid,formattime,generationnumber,updatetime,location_partition,location_startblock,prelocation_partition,prelocation_startblock,highestfileuid,current_height,TotalBytesProcessed,CurrentBytesProcessed,TotalBytesUnindexed)
                 values (@creator,@blocksize,@volumeuuid,@formattime,@generationnumber,@updatetime,@location_partition,@location_startblock,@prelocation_partition,@prelocation_startblock,@highestfileuid,@current_height,@TotalBytesProcessed,@CurrentBytesProcessed,@TotalBytesUnindexed)",
                connection
                )
            InsertCommand.Parameters.AddWithValue("@creator", plabel.creator)
            InsertCommand.Parameters.AddWithValue("@blocksize", plabel.blocksize)
            InsertCommand.Parameters.AddWithValue("@volumeuuid", plabel.volumeuuid.ToString())
            InsertCommand.Parameters.AddWithValue("@formattime", plabel.formattime)
            InsertCommand.Parameters.AddWithValue("@generationnumber", schema.generationnumber)
            InsertCommand.Parameters.AddWithValue("@updatetime", schema.updatetime)
            InsertCommand.Parameters.AddWithValue("@location_partition", schema.location.partition)
            InsertCommand.Parameters.AddWithValue("@location_startblock", schema.location.startblock)
            InsertCommand.Parameters.AddWithValue("@prelocation_partition", schema.previousgenerationlocation.partition)
            InsertCommand.Parameters.AddWithValue("@prelocation_startblock", schema.previousgenerationlocation.startblock)
            InsertCommand.Parameters.AddWithValue("@highestfileuid", schema.highestfileuid)
            InsertCommand.Parameters.AddWithValue("@current_height", currentHeight)
            InsertCommand.Parameters.AddWithValue("@TotalBytesProcessed", 0)
            InsertCommand.Parameters.AddWithValue("@CurrentBytesProcessed", 0)
            InsertCommand.Parameters.AddWithValue("@TotalBytesUnindexed", 0)
            
            LTFSWriter.FuncSqliteTrans(Sub()
                Metric.FuncFileOperationDuration(Sub()
                    insertCommand.ExecuteNonQuery()
                End Sub, {"", "Sqlite_InsertInitializeLTFSIndexInfo", ""})
            End Sub, BarCode)


        ElseIf ExistsResult > 1 Then
            Throw New Exception("ltfs_index_info table has more than one record")
        End If
    End Sub
    Public Class LTFSIndexInfoDto
        Public LTFSIndex As ltfsindex
        Public CurrentHeight As Int64
        Public TotalBytesUnindexed As Int64
    End Class
     

    Public Shared Function GetLTFSIndexInfo(connection As SQLiteConnection, BarCode As String)  As LTFSIndexInfoDto
        Dim ExistsCommand As New SQLiteCommand(
                    "SELECT * FROM ltfs_index_info ",
                    connection)
        Dim reader As SQLiteDataReader = ExistsCommand.ExecuteReader()
        Dim ltfsindex As ltfsindex= New ltfsindex
        Dim ltfsIndexInfo As ltfsindexInfoDto = New ltfsindexInfoDto
        
        if reader.Read() Then
            ltfsindex.creator = reader("creator")
            ltfsindex.volumeuuid = New Guid(reader("volumeuuid").ToString())
            ltfsindex.generationnumber = reader("generationnumber")
            ltfsindex.updatetime = reader("updatetime")
            ltfsindex.location.partition = reader("location_partition")
            ltfsindex.location.startblock = reader("location_startblock")
            ltfsindex.previousgenerationlocation.partition = reader("prelocation_partition")
            ltfsindex.previousgenerationlocation.startblock = reader("prelocation_startblock")
            ltfsindex.highestfileuid = reader("highestfileuid")
            ltfsIndexInfo.LTFSIndex=ltfsindex
            ltfsIndexInfo.CurrentHeight = reader("current_height")
            ltfsIndexInfo.TotalBytesUnindexed = reader("TotalBytesUnindexed")
            Return ltfsindexInfo
        End If
        Return Nothing
    End Function
    Public Shared Sub UpdateHightestFileUid(connection As SQLiteConnection, BarCode As String,highestfileuid As Int64)
        Dim ltfsindexInfoDto = GetLTFSIndexInfo(connection, BarCode)
        If ltfsindexInfoDto IsNot Nothing Then
            if ltfsindexInfoDto.LTFSIndex.highestfileuid >= highestfileuid Then
                throw new Exception($"highestfileuid is less than current highestfileuid :ltfsindexInfoDto.LTFSIndex.highestfileuid:{ltfsindexInfoDto.LTFSIndex.highestfileuid},highestfileuid:{highestfileuid}")
            End If
                Dim updateCommand As New SQLiteCommand("update ltfs_index_info set highestfileuid=@highestfileuid ",connection)
            updateCommand.Parameters.AddWithValue("@highestfileuid", highestfileuid)
            LTFSWriter.FuncSqliteTrans(Sub()
                    Metric.FuncFileOperationDuration(Sub()
                        updateCommand.ExecuteNonQuery()
                    End Sub, {"", "Sqlite_UpdateHightestFileUid", ""})
            End Sub,BarCode)
            
        Else 
                throw new Exception("ltfs_index_info table has no record")
        End If
    End Sub
    
    Public Shared Sub UpdateCurrentHeight(connection As SQLiteConnection, BarCode As String,currentHeight As Int64,totalBytesUnindexed As Int64)
        Dim ltfsindexInfoDto = GetLTFSIndexInfo(connection, BarCode)
        If ltfsindexInfoDto IsNot Nothing Then
            if ltfsindexInfoDto.currentHeight >= currentHeight Then
                throw new Exception($"currentHeight is less than current currentHeight,ltfsindexInfoDto.currentHeight:{ltfsindexInfoDto.currentHeight} ,currentHeight:{currentHeight}")
            End If
            Dim updateCommand As New SQLiteCommand("update ltfs_index_info set current_height=@current_height,TotalBytesUnindexed=@totalBytesUnindexed ",connection)
            updateCommand.Parameters.AddWithValue("@current_height", currentHeight)
            updateCommand.Parameters.AddWithValue("@totalBytesUnindexed", totalBytesUnindexed)
            LTFSWriter.FuncSqliteTrans(Sub()
                Metric.FuncFileOperationDuration(Sub()
                    updateCommand.ExecuteNonQuery()
                End Sub, {"", "Sqlite_Update_current_height_and_totalBytesUnindexed", ""})
            End Sub,BarCode)
            
        Else 
            throw new Exception("ltfs_index_info table has no record")
        End If
    End Sub
    Public Shared Sub UpdateCurrentHeight(connection As SQLiteConnection, BarCode As String,currentHeight As Int64)
        Dim ltfsindexInfoDto = GetLTFSIndexInfo(connection, BarCode)
        If ltfsindexInfoDto IsNot Nothing Then
            if ltfsindexInfoDto.currentHeight >= currentHeight Then
                throw new Exception($"currentHeight is less than current currentHeight,ltfsindexInfoDto.currentHeight:{ltfsindexInfoDto.currentHeight} ,currentHeight:{currentHeight}")
            End If
            Dim updateCommand As New SQLiteCommand("update ltfs_index_info set current_height=@current_height ",connection)
            updateCommand.Parameters.AddWithValue("@current_height", currentHeight)
            LTFSWriter.FuncSqliteTrans(Sub()
                Metric.FuncFileOperationDuration(Sub()
                    updateCommand.ExecuteNonQuery()
                End Sub, {"", "Sqlite_Update_current_height", ""})
            End Sub,BarCode)
            
        Else 
            throw new Exception("ltfs_index_info table has no record")
        End If
    End Sub
    ' 插入数据
    Public Shared Sub InsertFile(f As ltfsindex.file, parentDir As String, connection As SQLiteConnection,
                                 BarCode As String)
        ' 如果sqlite中数据不存在，才插入数据

        Dim archive = False
        For Each x As ltfsindex.file.xattr In f.extendedattributes
            If x.key = "ltfscopygui.archive" Then
                archive = True
            End If
        Next
        Dim fullpath = parentDir & "\" & f.name
        If archive Then
            fullpath = fullpath & ".ltfscopygui.archive"
        End If
        Dim ExistsCommand As New SQLiteCommand(
            "SELECT COUNT(*) FROM ltfs_index WHERE FullPath = @FullPath;",
            connection
            )
        ExistsCommand.Parameters.AddWithValue("@FullPath", fullpath)
        Dim ExistsResult As Integer = CInt(ExistsCommand.ExecuteScalar())
        If ExistsResult > 0 Then
            Metric.OperationCounter.WithLabels("sqlite_insert_file_exists").Inc()
            Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} {f.fullpath} 已经存在，跳过")
            Return
        End If
        Dim insertCommand As New SQLiteCommand(
            "INSERT INTO ltfs_index (
        FullPath, 
         ParentPath,
        Name, 
        ReadOnly, 
        OpenForWrite, 
        IsDirectory, 
        Length, 
        CreationTime, 
        ChangeTime, 
        ModifyTime, 
        AccessTime, 
        BackupTime, 
        SHA1, 
        FileUID, 
        StartBlock,
        extent, 
        extendedattributes
    ) VALUES (
        @FullPath, 
        @ParentPath,
        @Name, 
        @ReadOnly, 
        @OpenForWrite, 
        @IsDirectory, 
        @Length, 
        @CreationTime, 
        @ChangeTime, 
        @ModifyTime, 
        @AccessTime, 
        @BackupTime, 
        @SHA1, 
        @FileUID, 
        @StartBlock,
        @extent, 
        @extendedattributes
    );",
            connection
            )
        Dim startblock = Nothing
        If f.length > 0 AndAlso f.extentinfo.Count > 0 Then
            startblock = f.extentinfo(0).startblock
        End If

        f.fullpath = parentDir + f.fullpath
        ' 参数赋值，假设f对象包含相应的属性
        insertCommand.Parameters.AddWithValue("@FullPath", fullpath)
        insertCommand.Parameters.AddWithValue("@ParentPath", parentDir)
        insertCommand.Parameters.AddWithValue("@Name", f.name)
        insertCommand.Parameters.AddWithValue("@ReadOnly", f.readonly)
        insertCommand.Parameters.AddWithValue("@OpenForWrite", f.openforwrite)
        insertCommand.Parameters.AddWithValue("@IsDirectory", 0)
        insertCommand.Parameters.AddWithValue("@Length", f.length)
        insertCommand.Parameters.AddWithValue("@CreationTime", f.creationtime)
        insertCommand.Parameters.AddWithValue("@ChangeTime", f.changetime)
        insertCommand.Parameters.AddWithValue("@ModifyTime", f.modifytime)
        insertCommand.Parameters.AddWithValue("@AccessTime", f.accesstime)
        insertCommand.Parameters.AddWithValue("@BackupTime", f.backuptime)
        insertCommand.Parameters.AddWithValue("@SHA1", f.sha1)
        insertCommand.Parameters.AddWithValue("@FileUID", f.fileuid)
        insertCommand.Parameters.AddWithValue("@StartBlock", startblock)
        insertCommand.Parameters.AddWithValue("@extent", JsonConvert.SerializeObject(f.extentinfo))
        insertCommand.Parameters.AddWithValue("@extendedattributes", JsonConvert.SerializeObject(f.extendedattributes))

        ' 执行插入操作
        LTFSWriter.FuncSqliteTrans(Sub()
            Metric.FuncFileOperationDuration(Sub()
                try
                    insertCommand.ExecuteNonQuery()
                Catch ex As Exception
                    Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} {JsonConvert.SerializeObject(insertCommand.Parameters)} 插入失败，{ex.Message}")
                end try
            End Sub, {"", "Sqlite_InsertFile", ""})
        End Sub, BarCode)


        Metric.OperationCounter.WithLabels("sqlite_insert_file").Inc()
    End Sub


    ' 插入数据
    Public Shared Sub InsertDir(d As ltfsindex.directory, parentDir As String, connection As SQLiteConnection,
                                BarCode As String)

        ' 如果sqlite中数据不存在，才插入数据
        Dim ExistsCommand As New SQLiteCommand(
            "SELECT COUNT(*) FROM ltfs_index WHERE FullPath = @FullPath;",
            connection
            )
        If parentDir = "\" Then
            parentDir = ""
        End If
        Dim fullpath = parentDir & "\" & d.name
        ExistsCommand.Parameters.AddWithValue("@FullPath", fullpath)
        Dim ExistsResult As Integer = CInt(ExistsCommand.ExecuteScalar())
        If ExistsResult > 0 Then
            Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")} {fullpath} 已经存在，跳过")
            Metric.OperationCounter.WithLabels("sqlite_insert_dir_exists").Inc()
            Return
        End If
        Metric.OperationCounter.WithLabels("sqlite_insert_dir").Inc()

        Dim insertCommand As New SQLiteCommand(
            "INSERT INTO ltfs_index (
        FullPath, 
        ParentPath,
        Name, 
        IsDirectory, 
        Length, 
        CreationTime, 
        ChangeTime, 
        ModifyTime, 
        AccessTime, 
        BackupTime, 
       FileUID
    ) VALUES (
        @FullPath, 
        @ParentPath,
        @Name, 
        @IsDirectory, 
        @Length, 
        @CreationTime, 
        @ChangeTime, 
        @ModifyTime, 
        @AccessTime, 
        @BackupTime , 
                   @FileUID
    );",
            connection
            )

        ' 参数赋值，假设f对象包含相应的属性
        insertCommand.Parameters.AddWithValue("FullPath", fullpath)
        insertCommand.Parameters.AddWithValue("@ParentPath", parentDir)
        insertCommand.Parameters.AddWithValue("Name", d.name)
        insertCommand.Parameters.AddWithValue("IsDirectory", 1)
        insertCommand.Parameters.AddWithValue("Length", 0)
        insertCommand.Parameters.AddWithValue("CreationTime", d.creationtime)
        insertCommand.Parameters.AddWithValue("ChangeTime", d.changetime)
        insertCommand.Parameters.AddWithValue("ModifyTime", d.modifytime)
        insertCommand.Parameters.AddWithValue("AccessTime", d.accesstime)
        insertCommand.Parameters.AddWithValue("BackupTime", d.backuptime)
        insertCommand.Parameters.AddWithValue("@FileUID", d.fileuid)

        ' 执行插入操作
        LTFSWriter.FuncSqliteTrans(Sub()
            Metric.FuncFileOperationDuration(Sub()
                insertCommand.ExecuteNonQuery()
            End Sub, {"", "Sqlite_InsertDir", ""})
        End Sub, BarCode)
    End Sub

    Public Shared Sub HashSelectedDirWithSqlite(LW As LTFSWriter, selectedDir As ltfsindex.directory)

        Dim fc As Long = 0, ec As Long = 0
        LW.PrintMsg(My.Resources.ResText_Hashing)
        Dim conn As SQLiteConnection

        Try
            LW.StopFlag = False
            conn = DirProvider.CreateConnection($"sqlite\{LW.Barcode}.db")
            conn.Open()
            Dim c As Integer = 0
            Dim dir = DirProvider.QueryFileWithWhere($"fileuid={selectedDir.fileuid}", conn)
            DirProvider.QueryFilesSha1AndMd5(LW, $"{dir(0).fullpath}%", conn, Sub(f As ltfsindex.file, count As Integer)

                'LW.RestorePosition = New TapeUtils.PositionData(LW.TapeDrive)
                c += 1
                Metric.OperationCounter.WithLabels("HashSelectedDirWithSqlite").Inc()
                LW.PrintMsg(
                    $"{My.Resources.ResText_Hashing} [{c}/{count}] {Path.GetFileName(f.fullpath)} { _
                               My.Resources.ResText_Size}:{IOManager.FormatSize(f.length)}", False,
                    $"{My.Resources.ResText_Hashing} [{c}/{count}] {f.fullpath} { _
                               My.Resources.ResText_Size}:{f.length}")

                If LW.StopFlag Then
                    Throw New Exception("Stop")
                End If
                If _
                                                (f.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) = "") AndAlso
                                                (f.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) = "") Then
                    'skip
                    Threading.Interlocked.Add(LW.CurrentBytesProcessed, f.length)
                    Threading.Interlocked.Increment(LW.CurrentFilesProcessed)
                    Metric.OperationCounter.WithLabels("HashSelectedDirWithSqlite_skip").Inc()
                Else
                    Dim result As Dictionary(Of String, String) = LW.CalculateChecksum(f)
                    If result IsNot Nothing Then
                        If f.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) = result.Item("SHA1") _
                                                Then

                        ElseIf f.GetXAttr(ltfsindex.file.xattr.HashType.SHA1, True) <> "" Then

                            LW.PrintMsg(
                                $"SHA1 Mismatch at fileuid={f.fileuid} filename={f.fullpath} sha1logged={f.sha1} sha1calc={result.Item("SHA1")} size=｛f.length｝", ForceLog := True, Warning := True)
                            Threading.Interlocked.Increment(ec)
                        End If
                        If f.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) = result.Item("MD5") _
                                                Then
                        ElseIf f.GetXAttr(ltfsindex.file.xattr.HashType.MD5, True) <> "" Then
                            LW.PrintMsg(
                                $"SHA1 Mismatch at fileuid={f.fileuid} filename={f.fullpath} sha1logged={f.sha1} sha1calc={result.Item("SHA1")} size=｛f.length｝", ForceLog := True, Warning := True)
                            Threading.Interlocked.Increment(ec)
                        End If
                    End If
                End If
            End Sub)

        Catch ex As Exception
            LW.Invoke(Sub() MessageBox.Show(ex.ToString))
            LW.PrintMsg(My.Resources.ResText_HErr, Warning := True)
        End Try

        LW.UnwrittenSizeOverrideValue = 0
        LW.UnwrittenCountOverwriteValue = 0
    End Sub
    '查询文件信息
    Public Shared Sub QueryFilesSha1AndMd5(LW As LTFSWriter, pattern As String, connection As SQLiteConnection,
                                           action As Action(Of ltfsindex.file, Integer))
        Dim queryCountCommand As New SQLiteCommand(
            $"SELECT count(1)  FROM ltfs_index WHERE FullPath like '{pattern}' and IsDirectory=0 and length>0 ",
            connection)
        Dim count As Integer = CInt(queryCountCommand.ExecuteScalar())
        Dim stopFlag As Boolean = False
        Dim queryCommand As New SQLiteCommand(
            $"SELECT fileuid,FullPath,Length,extent,extendedattributes FROM ltfs_index WHERE FullPath like '{pattern _
                                                 }' and IsDirectory=0 and length>0 order by startblock asc",
            connection)

        Dim reader As SQLiteDataReader = queryCommand.ExecuteReader()
        LW.RestorePosition = New TapeUtils.PositionData(LW.TapeDrive)
        While reader.Read()
            Metric.OperationCounter.WithLabels("QueryFilesSha1AndMd5").Inc()
            Dim f As ltfsindex.file = New ltfsindex.file()
            f.fullpath = reader("FullPath").ToString()
            f.fileuid = reader("fileuid").ToString()

            If reader("Length") IsNot DBNull.Value Then
                f.length = Convert.ToInt64(reader("Length"))
                Metric.FuncFileOperationDuration(Sub()
                    f.extentinfo =
                                                    JsonConvert.DeserializeObject (Of List(Of ltfsindex.file.extent))(
                                                        reader("extent").ToString())
                    f.extendedattributes =
                                                    JsonConvert.DeserializeObject (Of List(Of ltfsindex.file.xattr))(
                                                        reader("extendedattributes"))
                End Sub, {"", "QueryFilesSha1AndMd5_DeserializeObject", ""})
            Else
                f.length = 0
            End If
            action(f, count)

        End While
    End Sub

    '查询文件信息
    Public Shared Function ReadDirWithWhere(where As String, connection As SQLiteConnection) As SortedList
        Dim queryCommand As New SQLiteCommand(
            $"SELECT * FROM ltfs_index WHERE {where}",
            connection
            )
        Dim lst As New SortedList()
        Dim reader As SQLiteDataReader
        Metric.FuncFileOperationDuration(Sub ()
            reader = queryCommand.ExecuteReader()
        End Sub, {"", "Sqlite_ReadDirWithWhere", ""})

        While reader.Read()
            Dim isDirectory As Boolean = Convert.ToBoolean(reader("IsDirectory"))
            If isDirectory Then
                Dim d = New ltfsindex.directory()
                d.fullpath = reader("FullPath").ToString()
                d.name = reader("Name").ToString()
                d.creationtime = Convert.ToString(reader("CreationTime"))
                d.changetime = Convert.ToString(reader("ChangeTime"))
                d.modifytime = Convert.ToString(reader("ModifyTime"))
                d.accesstime = Convert.ToString(reader("AccessTime"))
                d.backuptime = Convert.ToString(reader("BackupTime"))
                d.fileuid = Convert.ToInt64(reader("fileuid"))
                lst.Add(d.name, d)
            Else
                Dim f = New ltfsindex.file()
                f.fullpath = reader("FullPath").ToString()
                f.name = reader("Name").ToString()
                f.readonly = Convert.ToBoolean(reader("ReadOnly"))
                f.openforwrite = Convert.ToBoolean(reader("OpenForWrite"))
                f.creationtime = Convert.ToString(reader("CreationTime"))
                f.changetime = Convert.ToString(reader("ChangeTime"))
                f.modifytime = Convert.ToString(reader("ModifyTime"))
                f.accesstime = Convert.ToString(reader("AccessTime"))
                f.backuptime = Convert.ToString(reader("BackupTime"))
                f.fileuid = Convert.ToInt64(reader("fileuid"))
                If reader("Length") IsNot DBNull.Value Then
                    f.length = Convert.ToInt64(reader("Length"))
                    f.extentinfo =
                        JsonConvert.DeserializeObject (Of List(Of ltfsindex.file.extent))(reader("extent").ToString())
                    f.extendedattributes =
                        JsonConvert.DeserializeObject (Of List(Of ltfsindex.file.xattr))(reader("extendedattributes"))
                Else
                    f.length = 0

                End If
                lst.Add(f.name, f)
            End If

        End While
        Return lst
    End Function
    '查询文件信息
    Public Shared Function QueryFileWithWhere(where As String, connection As SQLiteConnection) As List(Of ltfsindex.file)
        Dim queryCommand As New SQLiteCommand(
            $"SELECT * FROM ltfs_index WHERE {where} ",
            connection
            )
        Dim reader As SQLiteDataReader
        Dim lst As New List(Of ltfsindex.file)
        Metric.FuncFileOperationDuration(Sub()
                                             reader = queryCommand.ExecuteReader()
                                         End Sub, {"", $"Sqlite_QueryFileWithWhere", ""})

        While reader.Read()
            Dim isDirectory As Boolean = Convert.ToBoolean(reader("IsDirectory"))
            If Not isDirectory Then

                Dim f As ltfsindex.file = New ltfsindex.file()
                f.fullpath = reader("FullPath").ToString()
                f.name = reader("Name").ToString()
                f.readonly = Convert.ToBoolean(reader("ReadOnly"))
                f.openforwrite = Convert.ToBoolean(reader("OpenForWrite"))
                f.creationtime = Convert.ToString(reader("CreationTime"))
                f.changetime = Convert.ToString(reader("ChangeTime"))
                f.modifytime = Convert.ToString(reader("ModifyTime"))
                f.accesstime = Convert.ToString(reader("AccessTime"))
                f.backuptime = Convert.ToString(reader("BackupTime"))
                f.fileuid = Convert.ToInt64(reader("fileuid"))
                f.isDirectory=Convert.ToBoolean(reader("isDirectory"))
                If reader("Length") IsNot DBNull.Value Then
                    f.length = Convert.ToInt64(reader("Length"))
                    f.extentinfo =
                        JsonConvert.DeserializeObject(Of List(Of ltfsindex.file.extent))(reader("extent").ToString())
                    f.extendedattributes =
                        JsonConvert.DeserializeObject(Of List(Of ltfsindex.file.xattr))(reader("extendedattributes"))
                Else
                    f.length = 0

                End If
                lst.Add(f)
            Else
                Dim f As ltfsindex.file = New ltfsindex.file()
                f.fullpath = reader("FullPath").ToString()
                f.name = reader("Name").ToString()
                f.creationtime = Convert.ToString(reader("CreationTime"))
                f.changetime = Convert.ToString(reader("ChangeTime"))
                f.modifytime = Convert.ToString(reader("ModifyTime"))
                f.accesstime = Convert.ToString(reader("AccessTime"))
                f.backuptime = Convert.ToString(reader("BackupTime"))
                f.fileuid = Convert.ToInt64(reader("fileuid"))
                f.isDirectory=Convert.ToBoolean(reader("isDirectory"))
                lst.Add(f)
            End If

        End While
        Return lst
    End Function

    Public Shared Function QueryFile(Path As String, connection As SQLiteConnection) As ltfsindex.file
        Dim files = QueryFileWithWhere($"FullPath='{Path}' and isdirectory=0", connection)
        if files.Count = 0 Then
            Return Nothing
        End If
        return files(0)
    End Function
    Public Shared Function QueryDirListWithWhere(where As String, connection As SQLiteConnection) As List(Of ltfsindex.directory)
        Dim result As New List(Of ltfsindex.directory)
        Dim files = QueryFileWithWhere($"{where}", connection)
        If files.Count = 0 Then
            Return New List(Of ltfsindex.directory)
        End If
        For Each file In files
            result.Add(ConvertFileToDir(file))
        Next
        Return result
    End Function
    Public Shared Function ConvertFileToDir(file As ltfsindex.file) As ltfsindex.directory
        Dim newdir As New ltfsindex.directory With {
                .name = file.name,
                .creationtime = file.creationtime,
                .fileuid = file.fileuid,
                .backuptime = file.backuptime,
                .accesstime = file.accesstime,
                .changetime = file.changetime,
                .modifytime = file.modifytime,
                .readonly = file.readonly,
                .fullpath = file.fullpath
                }
        Return newdir
    End Function
    Public Shared Function QueryDir(Path As String, connection As SQLiteConnection) As ltfsindex.directory

        Dim files = QueryFileWithWhere($"FullPath='{Path}' and isdirectory=1", connection)
        if files.Count = 0 Then
            Return Nothing
        End If
        Dim file = files(0)
        return ConvertFileToDir(file)
    End Function


    Public Shared Sub ImportSchemaToSqlite(LW As LTFSWriter)

        Dim tr As DbTransaction
        Try

            Dim conn = LW.GetSqliteConnection(LW.Barcode)
            DirProvider.CreateDatabaseAndTable(conn)

            tr = conn.BeginTransaction()
            Dim IterDir As Action(Of ltfsindex.directory, String, SQLiteConnection) =
                    Sub(tapeDir As ltfsindex.directory, outputDir As String, connection As SQLiteConnection)
                        For Each f As ltfsindex.file In tapeDir.contents._file
                            Try
                                InsertFile(f, outputDir, connection, LW.Barcode)
                            Catch ex As Exception
                                LW.PrintMsg($"InsertFile出错：{ex.ToString} {ex.StackTrace}", ForceLog:=True)
                            End Try
                            If _
                    Not f.GetXAttr("ltfscopygui.archive") Is Nothing AndAlso
                    f.GetXAttr("ltfscopygui.archive").ToLower = "true" Then
                                Try
                                    If Not IO.Directory.Exists(IO.Path.Combine(Application.StartupPath, "LDS_cache")) _
                    Then
                                        IO.Directory.CreateDirectory(IO.Path.Combine(Application.StartupPath,
                                                                                     "LDS_cache"))
                                    End If
                                    Dim tmpf As String = $"{Application.StartupPath}\LDS_cache\{f.sha1}.index.lds"
                                    If Not IO.File.Exists(tmpf) Then
                                        LW.RestorePosition = New TapeUtils.PositionData(LW.TapeDrive)
                                        LW.RestoreFile(tmpf, f)
                                    End If
                                    Dim dindex As ltfsindex.directory = ltfsindex.directory.FromFile(tmpf)
                                    Dim dirOutput As String = outputDir & "\" & dindex.name
                                    InsertDir(dindex, outputDir, connection, LW.Barcode)
                                    IterDir(dindex, dirOutput, connection)
                                Catch ex As Exception
                                    LW.PrintMsg($"解压索引出错：{ex.ToString} {ex.StackTrace}", ForceLog:=True)
                                End Try
                            End If
                            'RestoreFile(IO.Path.Combine(outputDir.FullName, f.name), f)
                        Next
                        For Each d As ltfsindex.directory In tapeDir.contents._directory
                            Dim dirOutput As String = outputDir & "\" & d.name
                            InsertDir(d, outputDir, connection, LW.Barcode)
                            IterDir(d, dirOutput, connection)
                        Next
                    End Sub
            For Each dir As ltfsindex.directory In LW.schema._directory
                IterDir(dir, "", conn)
            Next
            tr.Commit()
        Catch ex As Exception
            If Not tr Is Nothing Then
                tr.Rollback()
            End If
            LW.PrintMsg($"ImportSchemaToSqlite failed: {ex.Message}")
        End Try
    End Sub

    Public Shared Sub UpdateFileSize(ltfsfile As ltfsindex.file, connection As SQLiteConnection, BarCode As String)
        Dim queryCommand As New SQLiteCommand(
            $"update ltfs_index set length='{ltfsfile.length}'
            WHERE FullPath='{ltfsfile.fullpath}'",
            connection
            )
        LTFSWriter.FuncSqliteTrans(Sub()
            Metric.FuncFileOperationDuration(Sub()
                queryCommand.ExecuteNonQuery()
            End Sub, {"", "Sqlite_UpdateFileSize", ""})
        End Sub, BarCode)
    End Sub

    Public Shared Sub UpdateBaseInfo(ltfsfile As ltfsindex.file, connection As SQLiteConnection, BarCode As String)
        Dim queryCommand As New SQLiteCommand(
            $"update ltfs_index set creationtime='{ltfsfile.creationtime}', 
            modifytime='{ _
                                                 ltfsfile.modifytime}', 
            accesstime='{ltfsfile.accesstime _
                                                 }',
             changetime='{ltfsfile.changetime _
                                                 }'
             WHERE FullPath='{ltfsfile.fullpath}'",
            connection
            )
        LTFSWriter.FuncSqliteTrans(Sub()
            Metric.FuncFileOperationDuration(Sub()
                queryCommand.ExecuteNonQuery()
            End Sub, {"", "Sqlite_UpdateBaseInfo", ""})
        End Sub, BarCode)
    End Sub
    Public Shared Sub DeleteDir(Path As String, connection As SQLiteConnection, BarCode As String)
        Dim IterDeleteDir As Action(Of String) = Sub(ChildPath As String)
                                                     Dim dirs = QueryDirListWithWhere($"ParentPath='{ChildPath}' and isdirectory=1", connection)
                                                     For Each dir As ltfsindex.directory In dirs
                                                         IterDeleteDir(dir.fullpath)
'                                                         Dim sql=$"delete from ltfs_index WHERE ParentPath='{dir.fullpath}' "
'                                                         Console.WriteLine(sql)
'                                                         Dim queryCommand As New SQLiteCommand(
'                                                             sql,connection)
'                                                         LTFSWriter.FuncSqliteTrans(Sub()
'                                                                                        Metric.FuncFileOperationDuration(Sub()
'                                                                                                                             queryCommand.ExecuteNonQuery()
'                                                                                                                         End Sub, {"", "Sqlite_DeleteDir", ""})
'                                                                                    End Sub, BarCode)
                                                     Next
                                                     Dim sql2=$"delete from ltfs_index WHERE  FullPath='{ChildPath}' or ParentPath='{ChildPath}'"
                                                     Console.WriteLine(sql2)
                                                     Dim queryCommand2 As New SQLiteCommand(sql2, connection)
                                                     LTFSWriter.FuncSqliteTrans(Sub()
                                                                                    Metric.FuncFileOperationDuration(Sub()
                                                                                                                         queryCommand2.ExecuteNonQuery()
                                                                                                                     End Sub, {"", "Sqlite_DeleteDir", ""})
                                                                                End Sub, BarCode)
                                                 End Sub
        IterDeleteDir(Path)
    End Sub
    Public Shared Sub DeleteFile(ltfsFile As ltfsindex.file, connection As SQLiteConnection, BarCode As String)
        Dim queryCommand As New SQLiteCommand(
            $"delete from ltfs_index 
        WHERE FullPath='{ltfsFile.fullpath}'",
            connection
            )
        LTFSWriter.FuncSqliteTrans(Sub()
            Metric.FuncFileOperationDuration(Sub()
                queryCommand.ExecuteNonQuery()
            End Sub, {"", "Sqlite_DeleteFile", ""})
        End Sub, BarCode)
    End Sub
End Class