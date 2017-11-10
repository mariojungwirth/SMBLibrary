using System;
using System.Collections.Generic;
using System.IO;
using Utilities;

namespace SMBLibrary.FSFileStore.Adapter
{
    public sealed class FSFileStoreAdapter : INTFileStore
    {
        #region Variables

        private IFSFileStore _store;
        private const int BytesPerSector = 512;
        private const int ClusterSize = 4096;
        public event EventHandler<LogEntry> LogEntryAdded;

        #endregion

        #region Constructor

        public FSFileStoreAdapter(IFSFileStore store_)
        {
            _store = store_;
        }

        #endregion

        #region INTFileStore

        public NTStatus Cancel(object ioRequest_)
        {
            return NTStatus.STATUS_NOT_SUPPORTED;
        }

        public NTStatus CloseFile(object handle_)
        {
            FileHandle fileHandle = (FileHandle)handle_;
            if (fileHandle.Stream != null)
            {
                Log(Severity.Verbose, "CloseFile: Closing '{0}'.", fileHandle.Path);
                fileHandle.Stream.Close();
            }

            // If the file / directory was created with FILE_DELETE_ON_CLOSE but was not opened (with FileOptions.DeleteOnClose), we should delete it now.
            if (fileHandle.Stream == null && fileHandle.DeleteOnClose)
            {
                try
                {
                    if (fileHandle.IsDirectory)
                        _store.DeleteDirectory(fileHandle.Path);
                    else
                        _store.DeleteFile(fileHandle.Path);
                    Log(Severity.Verbose, "CloseFile: Deleted '{0}'.", fileHandle.Path);
                }
                catch
                {
                    Log(Severity.Verbose, "CloseFile: Error deleting '{0}'.", fileHandle.Path);
                }
            }

            return NTStatus.STATUS_SUCCESS;
        }

        public NTStatus CreateFile(out object handle_, out FileStatus fileStatus_, string path_, AccessMask desiredAccess_, FileAttributes fileAttributes_, ShareAccess shareAccess_, CreateDisposition createDisposition_, CreateOptions createOptions_, SecurityContext securityContext_)
        {
            handle_ = null;
            fileStatus_ = FileStatus.FILE_DOES_NOT_EXIST;
            FileAccess createAccess = NTFileStoreHelper.ToCreateFileAccess(desiredAccess_, createDisposition_);
            bool requestedWriteAccess = (createAccess & FileAccess.Write) > 0;

            bool forceDirectory = (createOptions_ & CreateOptions.FILE_DIRECTORY_FILE) > 0;
            bool forceFile = (createOptions_ & CreateOptions.FILE_NON_DIRECTORY_FILE) > 0;

            if (forceDirectory & (createDisposition_ != CreateDisposition.FILE_CREATE &&
                                  createDisposition_ != CreateDisposition.FILE_OPEN &&
                                  createDisposition_ != CreateDisposition.FILE_OPEN_IF &&
                                  createDisposition_ != CreateDisposition.FILE_SUPERSEDE))
            {
                return NTStatus.STATUS_INVALID_PARAMETER;
            }

            // Windows will try to access named streams (alternate data streams) regardless of the FILE_NAMED_STREAMS flag, we need to prevent this behaviour.
            if (path_.Contains(":"))
            {
                // Windows Server 2003 will return STATUS_OBJECT_NAME_NOT_FOUND
                return NTStatus.STATUS_NO_SUCH_FILE;
            }

            FileSystemEntry entry;
            try
            {
                entry = _store.GetEntry(path_);
            }
            catch (Exception ex)
            {
                NTStatus status = ToNTStatus(ex);
                Log(Severity.Verbose, "CreateFile: Error retrieving '{0}'. {1}.", path_, status);
                return status;
            }

            if (createDisposition_ == CreateDisposition.FILE_OPEN)
            {
                if (entry == null)
                {
                    return NTStatus.STATUS_NO_SUCH_FILE;
                }

                fileStatus_ = FileStatus.FILE_EXISTS;
                if (entry.IsDirectory && forceFile)
                {
                    return NTStatus.STATUS_FILE_IS_A_DIRECTORY;
                }

                if (!entry.IsDirectory && forceDirectory)
                {
                    return NTStatus.STATUS_OBJECT_PATH_INVALID;
                }
            }
            else if (createDisposition_ == CreateDisposition.FILE_CREATE)
            {
                if (entry != null)
                {
                    // File already exists, fail the request
                    Log(Severity.Verbose, "CreateFile: File '{0}' already exists.", path_);
                    fileStatus_ = FileStatus.FILE_EXISTS;
                    return NTStatus.STATUS_OBJECT_NAME_COLLISION;
                }

                if (!requestedWriteAccess)
                {
                    return NTStatus.STATUS_ACCESS_DENIED;
                }

                try
                {
                    if (forceDirectory)
                    {
                        Log(Severity.Information, "CreateFile: Creating directory '{0}'", path_);
                        entry = _store.CreateDirectory(path_);
                    }
                    else
                    {
                        Log(Severity.Information, "CreateFile: Creating file '{0}'", path_);
                        entry = _store.CreateFile(path_);
                    }
                }
                catch (Exception ex)
                {
                    NTStatus status = ToNTStatus(ex);
                    Log(Severity.Verbose, "CreateFile: Error creating '{0}'. {1}.", path_, status);
                    return status;
                }
                fileStatus_ = FileStatus.FILE_CREATED;
            }
            else if (createDisposition_ == CreateDisposition.FILE_OPEN_IF ||
                     createDisposition_ == CreateDisposition.FILE_OVERWRITE ||
                     createDisposition_ == CreateDisposition.FILE_OVERWRITE_IF ||
                     createDisposition_ == CreateDisposition.FILE_SUPERSEDE)
            {
                if (entry == null)
                {
                    if (createDisposition_ == CreateDisposition.FILE_OVERWRITE)
                    {
                        return NTStatus.STATUS_OBJECT_PATH_NOT_FOUND;
                    }

                    if (!requestedWriteAccess)
                    {
                        return NTStatus.STATUS_ACCESS_DENIED;
                    }

                    try
                    {
                        if (forceDirectory)
                        {
                            Log(Severity.Information, "CreateFile: Creating directory '{0}'", path_);
                            entry = _store.CreateDirectory(path_);
                        }
                        else
                        {
                            Log(Severity.Information, "CreateFile: Creating file '{0}'", path_);
                            entry = _store.CreateFile(path_);
                        }
                    }
                    catch (Exception ex)
                    {
                        NTStatus status = ToNTStatus(ex);
                        Log(Severity.Verbose, "CreateFile: Error creating '{0}'. {1}.", path_, status);
                        return status;
                    }
                    fileStatus_ = FileStatus.FILE_CREATED;
                }
                else
                {
                    fileStatus_ = FileStatus.FILE_EXISTS;
                    if (createDisposition_ == CreateDisposition.FILE_OPEN_IF)
                    {
                        if (entry.IsDirectory && forceFile)
                        {
                            return NTStatus.STATUS_FILE_IS_A_DIRECTORY;
                        }

                        if (!entry.IsDirectory && forceDirectory)
                        {
                            return NTStatus.STATUS_OBJECT_PATH_INVALID;
                        }
                    }
                    else
                    {
                        if (!requestedWriteAccess)
                        {
                            return NTStatus.STATUS_ACCESS_DENIED;
                        }

                        if (createDisposition_ == CreateDisposition.FILE_OVERWRITE ||
                            createDisposition_ == CreateDisposition.FILE_OVERWRITE_IF)
                        {
                            // Truncate the file
                            try
                            {
                                Stream temp = _store.OpenFile(path_, FileMode.Truncate, FileAccess.ReadWrite, FileShare.ReadWrite, FileOptions.None);
                                temp.Close();
                            }
                            catch (Exception ex)
                            {
                                NTStatus status = ToNTStatus(ex);
                                Log(Severity.Verbose, "CreateFile: Error truncating '{0}'. {1}.", path_, status);
                                return status;
                            }
                            fileStatus_ = FileStatus.FILE_OVERWRITTEN;
                        }
                        else if (createDisposition_ == CreateDisposition.FILE_SUPERSEDE)
                        {
                            // Delete the old file
                            try
                            {
                                _store.DeleteFile(path_);
                            }
                            catch (Exception ex)
                            {
                                NTStatus status = ToNTStatus(ex);
                                Log(Severity.Verbose, "CreateFile: Error deleting '{0}'. {1}.", path_, status);
                                return status;
                            }

                            try
                            {
                                if (forceDirectory)
                                {
                                    Log(Severity.Information, "CreateFile: Creating directory '{0}'", path_);
                                    entry = _store.CreateDirectory(path_);
                                }
                                else
                                {
                                    Log(Severity.Information, "CreateFile: Creating file '{0}'", path_);
                                    entry = _store.CreateFile(path_);
                                }
                            }
                            catch (Exception ex)
                            {
                                NTStatus status = ToNTStatus(ex);
                                Log(Severity.Verbose, "CreateFile: Error creating '{0}'. {1}.", path_, status);
                                return status;
                            }
                            fileStatus_ = FileStatus.FILE_SUPERSEDED;
                        }
                    }
                }
            }
            else
            {
                return NTStatus.STATUS_INVALID_PARAMETER;
            }

            FileAccess fileAccess = NTFileStoreHelper.ToFileAccess(desiredAccess_);
            Stream stream;
            if (fileAccess == (FileAccess)0 || entry.IsDirectory)
            {
                stream = null;
            }
            else
            {
                FileShare fileShare = NTFileStoreHelper.ToFileShare(shareAccess_);
                FileOptions fileOptions = ToFileOptions(createOptions_);
                string fileShareString = fileShare.ToString().Replace(", ", "|");
                string fileOptionsString = ToFileOptionsString(fileOptions);
                try
                {
                    stream = _store.OpenFile(path_, FileMode.Open, fileAccess, fileShare, fileOptions);
                }
                catch (Exception ex)
                {
                    NTStatus status = ToNTStatus(ex);
                    Log(Severity.Verbose, "OpenFile: Cannot open '{0}', Access={1}, Share={2}. NTStatus: {3}.", path_, fileAccess, fileShareString, status);
                    return status;
                }
            }

            bool deleteOnClose = (createOptions_ & CreateOptions.FILE_DELETE_ON_CLOSE) > 0;
            handle_ = new FileHandle(path_, entry.IsDirectory, stream, deleteOnClose);
            if (fileStatus_ != FileStatus.FILE_CREATED &&
                fileStatus_ != FileStatus.FILE_OVERWRITTEN &&
                fileStatus_ != FileStatus.FILE_SUPERSEDED)
            {
                fileStatus_ = FileStatus.FILE_OPENED;
            }
            return NTStatus.STATUS_SUCCESS;
        }

        public NTStatus DeviceIOControl(object handle_, uint ctlCode_, byte[] input_, out byte[] output_, int maxOutputLength_)
        {
            output_ = null;
            return NTStatus.STATUS_NOT_SUPPORTED;
        }

        public NTStatus FlushFileBuffers(object handle_)
        {
            FileHandle fileHandle = (FileHandle)handle_;
            if (fileHandle.Stream != null)
                fileHandle.Stream.Flush();

            return NTStatus.STATUS_SUCCESS;
        }

        public NTStatus GetFileInformation(out FileInformation result_, object handle_, FileInformationClass informationClass_)
        {
            FileHandle fileHandle = (FileHandle)handle_;
            string path = fileHandle.Path;
            FileSystemEntry entry;
            try
            {
                entry = _store.GetEntry(path);
            }
            catch (Exception ex)
            {
                NTStatus status = ToNTStatus(ex);
                Log(Severity.Verbose, "GetFileInformation on '{0}' failed. {1}", path, status);
                result_ = null;
                return status;
            }

            if (entry == null)
            {
                result_ = null;
                return NTStatus.STATUS_NO_SUCH_FILE;
            }

            switch (informationClass_)
            {
                case FileInformationClass.FileBasicInformation:
                    {
                        FileBasicInformation information = new FileBasicInformation();
                        information.CreationTime = entry.CreationTime;
                        information.LastAccessTime = entry.LastAccessTime;
                        information.LastWriteTime = entry.LastWriteTime;
                        information.ChangeTime = entry.LastWriteTime;
                        information.FileAttributes = GetFileAttributes(entry);
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                case FileInformationClass.FileStandardInformation:
                    {
                        FileStandardInformation information = new FileStandardInformation();
                        information.AllocationSize = (long)GetAllocationSize(entry.Size);
                        information.EndOfFile = (long)entry.Size;
                        information.Directory = entry.IsDirectory;
                        information.DeletePending = fileHandle.DeleteOnClose;
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                case FileInformationClass.FileInternalInformation:
                    {
                        FileInternalInformation information = new FileInternalInformation();
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                case FileInformationClass.FileEaInformation:
                    {
                        FileEaInformation information = new FileEaInformation();
                        information.EaSize = 0;
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                case FileInformationClass.FileAccessInformation:
                    {
                        result_ = null;
                        return NTStatus.STATUS_NOT_IMPLEMENTED;
                    }
                case FileInformationClass.FileNameInformation:
                    {
                        FileNameInformation information = new FileNameInformation();
                        information.FileName = entry.Name;
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                case FileInformationClass.FilePositionInformation:
                    {
                        result_ = null;
                        return NTStatus.STATUS_NOT_IMPLEMENTED;
                    }
                case FileInformationClass.FileFullEaInformation:
                    {
                        result_ = null;
                        return NTStatus.STATUS_NOT_IMPLEMENTED;
                    }
                case FileInformationClass.FileModeInformation:
                    {
                        result_ = null;
                        return NTStatus.STATUS_NOT_IMPLEMENTED;
                    }
                case FileInformationClass.FileAlignmentInformation:
                    {
                        result_ = null;
                        return NTStatus.STATUS_NOT_IMPLEMENTED;
                    }
                case FileInformationClass.FileAllInformation:
                    {
                        FileAllInformation information = new FileAllInformation();
                        information.BasicInformation.CreationTime = entry.CreationTime;
                        information.BasicInformation.LastAccessTime = entry.LastAccessTime;
                        information.BasicInformation.LastWriteTime = entry.LastWriteTime;
                        information.BasicInformation.ChangeTime = entry.LastWriteTime;
                        information.BasicInformation.FileAttributes = GetFileAttributes(entry);
                        information.StandardInformation.AllocationSize = (long)GetAllocationSize(entry.Size);
                        information.StandardInformation.EndOfFile = (long)entry.Size;
                        information.StandardInformation.Directory = entry.IsDirectory;
                        information.StandardInformation.DeletePending = fileHandle.DeleteOnClose;
                        information.NameInformation.FileName = entry.Name;
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                case FileInformationClass.FileAlternateNameInformation:
                    {
                        result_ = null;
                        return NTStatus.STATUS_NOT_IMPLEMENTED;
                    }
                case FileInformationClass.FileStreamInformation:
                    {
                        // This information class is used to enumerate the data streams of a file or a directory.
                        // A buffer of FileStreamInformation data elements is returned by the server.
                        FileStreamInformation information = new FileStreamInformation();
                        FileStreamEntry streamEntry = new FileStreamEntry();
                        streamEntry.StreamSize = (long)entry.Size;
                        streamEntry.StreamAllocationSize = (long)GetAllocationSize(entry.Size);
                        streamEntry.StreamName = "::$DATA";
                        information.Entries.Add(streamEntry);
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                case FileInformationClass.FilePipeInformation:
                    {
                        result_ = null;
                        return NTStatus.STATUS_NOT_IMPLEMENTED;
                    }
                case FileInformationClass.FilePipeLocalInformation:
                    {
                        result_ = null;
                        return NTStatus.STATUS_NOT_IMPLEMENTED;
                    }
                case FileInformationClass.FilePipeRemoteInformation:
                    {
                        result_ = null;
                        return NTStatus.STATUS_NOT_IMPLEMENTED;
                    }
                case FileInformationClass.FileCompressionInformation:
                    {
                        result_ = null;
                        return NTStatus.STATUS_NOT_IMPLEMENTED;
                    }
                case FileInformationClass.FileNetworkOpenInformation:
                    {
                        FileNetworkOpenInformation information = new FileNetworkOpenInformation();
                        information.CreationTime = entry.CreationTime;
                        information.LastAccessTime = entry.LastAccessTime;
                        information.LastWriteTime = entry.LastWriteTime;
                        information.ChangeTime = entry.LastWriteTime;
                        information.AllocationSize = (long)GetAllocationSize(entry.Size);
                        information.EndOfFile = (long)entry.Size;
                        information.FileAttributes = GetFileAttributes(entry);
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                case FileInformationClass.FileAttributeTagInformation:
                    {
                        result_ = null;
                        return NTStatus.STATUS_NOT_IMPLEMENTED;
                    }
                default:
                    result_ = null;
                    return NTStatus.STATUS_INVALID_INFO_CLASS;
            }
        }

        public NTStatus GetFileSystemInformation(out FileSystemInformation result_, FileSystemInformationClass informationClass_)
        {
            switch (informationClass_)
            {
                case FileSystemInformationClass.FileFsVolumeInformation:
                    {
                        FileFsVolumeInformation information = new FileFsVolumeInformation();
                        information.SupportsObjects = false;
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                case FileSystemInformationClass.FileFsSizeInformation:
                    {
                        FileFsSizeInformation information = new FileFsSizeInformation();
                        information.TotalAllocationUnits = -_store.Size / ClusterSize;
                        information.AvailableAllocationUnits = _store.FreeSpace / ClusterSize;
                        information.SectorsPerAllocationUnit = ClusterSize / BytesPerSector;
                        information.BytesPerSector = BytesPerSector;
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                case FileSystemInformationClass.FileFsDeviceInformation:
                    {
                        FileFsDeviceInformation information = new FileFsDeviceInformation();
                        information.DeviceType = DeviceType.Disk;
                        information.Characteristics = DeviceCharacteristics.IsMounted;
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                case FileSystemInformationClass.FileFsAttributeInformation:
                    {
                        FileFsAttributeInformation information = new FileFsAttributeInformation();
                        information.FileSystemAttributes = FileSystemAttributes.UnicodeOnDisk;
                        information.MaximumComponentNameLength = 255;
                        information.FileSystemName = _store.Name;
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                case FileSystemInformationClass.FileFsControlInformation:
                    {
                        FileFsControlInformation information = new FileFsControlInformation();
                        information.FileSystemControlFlags = FileSystemControlFlags.ContentIndexingDisabled;
                        information.DefaultQuotaThreshold = UInt64.MaxValue;
                        information.DefaultQuotaLimit = UInt64.MaxValue;
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                case FileSystemInformationClass.FileFsFullSizeInformation:
                    {
                        FileFsFullSizeInformation information = new FileFsFullSizeInformation();
                        information.TotalAllocationUnits = _store.Size / ClusterSize;
                        information.CallerAvailableAllocationUnits = _store.FreeSpace / ClusterSize;
                        information.ActualAvailableAllocationUnits = _store.FreeSpace / ClusterSize;
                        information.SectorsPerAllocationUnit = ClusterSize / BytesPerSector;
                        information.BytesPerSector = BytesPerSector;
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                case FileSystemInformationClass.FileFsObjectIdInformation:
                    {
                        result_ = null;
                        // STATUS_INVALID_PARAMETER is returned when the file system does not implement object IDs
                        // See: https://msdn.microsoft.com/en-us/library/cc232106.aspx
                        return NTStatus.STATUS_INVALID_PARAMETER;
                    }
                case FileSystemInformationClass.FileFsSectorSizeInformation:
                    {
                        FileFsSectorSizeInformation information = new FileFsSectorSizeInformation();
                        information.LogicalBytesPerSector = BytesPerSector;
                        information.PhysicalBytesPerSectorForAtomicity = BytesPerSector;
                        information.PhysicalBytesPerSectorForPerformance = BytesPerSector;
                        information.FileSystemEffectivePhysicalBytesPerSectorForAtomicity = BytesPerSector;
                        information.ByteOffsetForSectorAlignment = 0;
                        information.ByteOffsetForPartitionAlignment = 0;
                        result_ = information;
                        return NTStatus.STATUS_SUCCESS;
                    }
                default:
                    {
                        result_ = null;
                        return NTStatus.STATUS_INVALID_INFO_CLASS;
                    }
            }
        }

        public NTStatus GetSecurityInformation(out SecurityDescriptor result_, object handle_, SecurityInformation securityInformation_)
        {
            result_ = null;
            return NTStatus.STATUS_NOT_SUPPORTED;
        }

        public NTStatus LockFile(object handle_, long byteOffset_, long length_, bool exclusiveLock_)
        {
            return NTStatus.STATUS_NOT_SUPPORTED;
        }

        public NTStatus NotifyChange(out object ioRequest_, object handle_, NotifyChangeFilter completionFilter_, bool watchTree_, int outputBufferSize_, OnNotifyChangeCompleted onNotifyChangeCompleted_, object context_)
        {
            ioRequest_ = null;
            return NTStatus.STATUS_NOT_SUPPORTED;
        }

        public NTStatus QueryDirectory(out List<QueryDirectoryFileInformation> result_, object handle_, string fileName_, FileInformationClass informationClass_)
        {
            result_ = null;
            FileHandle directoryHandle = (FileHandle)handle_;
            if (!directoryHandle.IsDirectory)
            {
                return NTStatus.STATUS_INVALID_PARAMETER;
            }

            if (fileName_ == String.Empty)
            {
                return NTStatus.STATUS_INVALID_PARAMETER;
            }

            string path = directoryHandle.Path;
            bool findExactName = !ContainsWildcardCharacters(fileName_);

            List<FileSystemEntry> entries;
            if (!findExactName)
            {
                try
                {
                    entries = _store.ListEntriesInDirectory(path);
                }
                catch (UnauthorizedAccessException)
                {
                    return NTStatus.STATUS_ACCESS_DENIED;
                }

                entries = GetFiltered(entries, fileName_);

                // Windows will return "." and ".." when enumerating directory files.
                // The SMB1 / SMB2 specifications mandate that when zero entries are found, the server SHOULD / MUST return STATUS_NO_SUCH_FILE.
                // For this reason, we MUST include the current directory and/or parent directory when enumerating a directory
                // in order to diffrentiate between a directory that does not exist and a directory with no entries.
                FileSystemEntry currentDirectory = _store.GetEntry(path).Clone();
                currentDirectory.Name = ".";
                FileSystemEntry parentDirectory = _store.GetParentDirectory(FileSystem.GetParentDirectory(path)).Clone();
                parentDirectory.Name = "..";
                entries.Insert(0, parentDirectory);
                entries.Insert(0, currentDirectory);
            }
            else
            {
                path = FileSystem.GetDirectoryPath(path);
                FileSystemEntry entry = _store.GetEntry(path + fileName_);
                if (entry == null)
                {
                    return NTStatus.STATUS_NO_SUCH_FILE;
                }
                entries = new List<FileSystemEntry>();
                entries.Add(entry);
            }

            try
            {
                result_ = FromFileSystemEntries(entries, informationClass_);
            }
            catch (UnsupportedInformationLevelException)
            {
                return NTStatus.STATUS_INVALID_INFO_CLASS;
            }
            return NTStatus.STATUS_SUCCESS;
        }

        public NTStatus ReadFile(out byte[] data_, object handle_, long offset_, int maxCount_)
        {
            data_ = null;
            FileHandle fileHandle = (FileHandle)handle_;
            string path = fileHandle.Path;
            Stream stream = fileHandle.Stream;
            if (stream == null || !stream.CanRead)
            {
                Log(Severity.Verbose, "ReadFile: Cannot read '{0}', Invalid Operation.", path);
                return NTStatus.STATUS_ACCESS_DENIED;
            }

            int bytesRead;
            try
            {
                if(stream.Position != offset_)
                    stream.Seek(offset_, SeekOrigin.Begin);

                data_ = new byte[maxCount_];
                bytesRead = stream.Read(data_, 0, maxCount_);
            }
            catch (Exception ex)
            {
                NTStatus status = ToNTStatus(ex);
                Log(Severity.Verbose, "ReadFile: Cannot read '{0}'. {1}.", path, status);
                return status;
            }

            if (bytesRead < maxCount_)
            {
                // EOF, we must trim the response data array
                data_ = ByteReader.ReadBytes(data_, 0, bytesRead);
            }
            return NTStatus.STATUS_SUCCESS;
        }

        public NTStatus SetFileInformation(object handle_, FileInformation information_)
        {
            FileHandle fileHandle = (FileHandle)handle_;
            if (information_ is FileBasicInformation)
            {
                FileBasicInformation basicInformation = (FileBasicInformation)information_;
                bool isHidden = ((basicInformation.FileAttributes & FileAttributes.Hidden) > 0);
                bool isReadonly = (basicInformation.FileAttributes & FileAttributes.ReadOnly) > 0;
                bool isArchived = (basicInformation.FileAttributes & FileAttributes.Archive) > 0;
                try
                {
                    if(fileHandle.IsDirectory)
                        _store.SetAttributesDirectory(fileHandle.Path, isHidden, isReadonly, isArchived);
                    else
                        _store.SetAttributesFile(fileHandle.Path, isHidden, isReadonly, isArchived);
                }
                catch (Exception ex)
                {
                    NTStatus status = ToNTStatus(ex);
                    Log(Severity.Verbose, "SetFileInformation: Failed to set file attributes on '{0}'. {1}.", fileHandle.Path, status);
                    return status;
                }

                try
                {
                    if(fileHandle.IsDirectory)
                        _store.SetDatesDirectory(fileHandle.Path, basicInformation.CreationTime, basicInformation.LastWriteTime, basicInformation.LastAccessTime);
                    else
                        _store.SetDatesFile(fileHandle.Path, basicInformation.CreationTime, basicInformation.LastWriteTime, basicInformation.LastAccessTime);
                }
                catch (Exception ex)
                {
                    NTStatus status = ToNTStatus(ex);
                    Log(Severity.Verbose, "SetFileInformation: Failed to set file dates on '{0}'. {1}.", fileHandle.Path, status);
                    return status;
                }
                return NTStatus.STATUS_SUCCESS;
            }
            else if (information_ is FileRenameInformationType2)
            {
                FileRenameInformationType2 renameInformation = (FileRenameInformationType2)information_;
                string newFileName = renameInformation.FileName;
                if (!newFileName.StartsWith(@"\"))
                {
                    newFileName = @"\" + newFileName;
                }

                if (fileHandle.Stream != null)
                {
                    fileHandle.Stream.Close();
                }

                // Note: it's possible that we just want to upcase / downcase a filename letter.
                try
                {
                    if (renameInformation.ReplaceIfExists && (_store.GetEntry(newFileName) != null))
                    {
                        if (fileHandle.IsDirectory)
                            _store.DeleteDirectory(newFileName);
                        else
                            _store.DeleteFile(newFileName);
                    }

                    if(fileHandle.IsDirectory)
                        _store.MoveDirectory(fileHandle.Path, newFileName);
                    else
                        _store.MoveFile(fileHandle.Path, newFileName);
                    Log(Severity.Information, "SetFileInformation: Renamed '{0}' to '{1}'", fileHandle.Path, newFileName);
                }
                catch (Exception ex)
                {
                    NTStatus status = ToNTStatus(ex);
                    Log(Severity.Verbose, "SetFileInformation: Cannot rename '{0}' to '{1}'. {2}.", fileHandle.Path, newFileName, status);
                    return status;
                }
                fileHandle.Path = newFileName;
                return NTStatus.STATUS_SUCCESS;
            }
            else if (information_ is FileDispositionInformation)
            {
                if (((FileDispositionInformation)information_).DeletePending)
                {
                    // We're supposed to delete the file on close, but it's too late to report errors at this late stage
                    if (fileHandle.Stream != null)
                    {
                        fileHandle.Stream.Close();
                    }

                    try
                    {
                        _store.DeleteFile(fileHandle.Path);
                        Log(Severity.Information, "SetFileInformation: Deleted '{0}'", fileHandle.Path);
                    }
                    catch (Exception ex)
                    {
                        NTStatus status = ToNTStatus(ex);
                        Log(Severity.Information, "SetFileInformation: Error deleting '{0}'. {1}.", fileHandle.Path, status);
                        return status;
                    }
                }
                return NTStatus.STATUS_SUCCESS;
            }
            else if (information_ is FileAllocationInformation)
            {
                long allocationSize = ((FileAllocationInformation)information_).AllocationSize;
                try
                {
                    fileHandle.Stream.SetLength(allocationSize);
                }
                catch (Exception ex)
                {
                    NTStatus status = ToNTStatus(ex);
                    Log(Severity.Verbose, "SetFileInformation: Cannot set allocation for '{0}'. {1}.", fileHandle.Path, status);
                    return status;
                }
                return NTStatus.STATUS_SUCCESS;
            }
            else if (information_ is FileEndOfFileInformation)
            {
                long endOfFile = ((FileEndOfFileInformation)information_).EndOfFile;
                try
                {
                    fileHandle.Stream.SetLength(endOfFile);
                }
                catch (Exception ex)
                {
                    NTStatus status = ToNTStatus(ex);
                    Log(Severity.Verbose, "SetFileInformation: Cannot set end of file for '{0}'. {1}.", fileHandle.Path, status);
                    return status;
                }
                return NTStatus.STATUS_SUCCESS;
            }
            else
            {
                return NTStatus.STATUS_NOT_IMPLEMENTED;
            }
        }

        public NTStatus SetFileSystemInformation(FileSystemInformation information)
        {
            return NTStatus.STATUS_NOT_SUPPORTED;
        }

        public NTStatus SetSecurityInformation(object handle_, SecurityInformation securityInformation_, SecurityDescriptor securityDescriptor_)
        {
            return NTStatus.STATUS_NOT_SUPPORTED;
        }

        public NTStatus UnlockFile(object handle_, long byteOffset_, long length_)
        {
            return NTStatus.STATUS_NOT_SUPPORTED;
        }

        public NTStatus WriteFile(out int numberOfBytesWritten_, object handle_, long offset_, byte[] data_)
        {
            numberOfBytesWritten_ = 0;
            FileHandle fileHandle = (FileHandle)handle_;
            string path = fileHandle.Path;
            Stream stream = fileHandle.Stream;
            if (stream == null || !stream.CanWrite)
            {
                Log(Severity.Verbose, "WriteFile: Cannot write '{0}'. Invalid Operation.", path);
                return NTStatus.STATUS_ACCESS_DENIED;
            }

            try
            {
                if(stream.Position != offset_)
                    stream.Seek(offset_, SeekOrigin.Begin);

                stream.Write(data_, 0, data_.Length);
            }
            catch (Exception ex)
            {
                NTStatus status = ToNTStatus(ex);
                Log(Severity.Verbose, "WriteFile: Cannot write '{0}'. {1}.", path, status);
                return status;
            }
            numberOfBytesWritten_ = data_.Length;
            return NTStatus.STATUS_SUCCESS;
        }

        #endregion

        #region Private Functions

        private static string GetShortName(string fileName_)
        {
            string fileNameWithoutExt = Path.GetFileNameWithoutExtension(fileName_);
            string extension = Path.GetExtension(fileName_);
            if (fileNameWithoutExt.Length > 8 || extension.Length > 4)
            {
                if (fileNameWithoutExt.Length > 8)
                {
                    fileNameWithoutExt = fileNameWithoutExt.Substring(0, 8);
                }

                if (extension.Length > 4)
                {
                    extension = extension.Substring(0, 4);
                }

                return fileNameWithoutExt + extension;
            }
            else
            {
                return fileName_;
            }
        }

        private static List<QueryDirectoryFileInformation> FromFileSystemEntries(List<FileSystemEntry> entries_, FileInformationClass informationClass_)
        {
            List<QueryDirectoryFileInformation> result = new List<QueryDirectoryFileInformation>();
            foreach (FileSystemEntry entry in entries_)
            {
                QueryDirectoryFileInformation information = FromFileSystemEntry(entry, informationClass_);
                result.Add(information);
            }
            return result;
        }

        private static QueryDirectoryFileInformation FromFileSystemEntry(FileSystemEntry entry_, FileInformationClass informationClass_)
        {
            switch (informationClass_)
            {
                case FileInformationClass.FileBothDirectoryInformation:
                    {
                        FileBothDirectoryInformation result = new FileBothDirectoryInformation();
                        result.CreationTime = entry_.CreationTime;
                        result.LastAccessTime = entry_.LastAccessTime;
                        result.LastWriteTime = entry_.LastWriteTime;
                        result.ChangeTime = entry_.LastWriteTime;
                        result.EndOfFile = (long)entry_.Size;
                        result.AllocationSize = (long)GetAllocationSize(entry_.Size);
                        result.FileAttributes = GetFileAttributes(entry_);
                        result.EaSize = 0;
                        result.ShortName = GetShortName(entry_.Name);
                        result.FileName = entry_.Name;
                        return result;
                    }
                case FileInformationClass.FileDirectoryInformation:
                    {
                        FileDirectoryInformation result = new FileDirectoryInformation();
                        result.CreationTime = entry_.CreationTime;
                        result.LastAccessTime = entry_.LastAccessTime;
                        result.LastWriteTime = entry_.LastWriteTime;
                        result.ChangeTime = entry_.LastWriteTime;
                        result.EndOfFile = (long)entry_.Size;
                        result.AllocationSize = (long)GetAllocationSize(entry_.Size);
                        result.FileAttributes = GetFileAttributes(entry_);
                        result.FileName = entry_.Name;
                        return result;
                    }
                case FileInformationClass.FileFullDirectoryInformation:
                    {
                        FileFullDirectoryInformation result = new FileFullDirectoryInformation();
                        result.CreationTime = entry_.CreationTime;
                        result.LastAccessTime = entry_.LastAccessTime;
                        result.LastWriteTime = entry_.LastWriteTime;
                        result.ChangeTime = entry_.LastWriteTime;
                        result.EndOfFile = (long)entry_.Size;
                        result.AllocationSize = (long)GetAllocationSize(entry_.Size);
                        result.FileAttributes = GetFileAttributes(entry_);
                        result.EaSize = 0;
                        result.FileName = entry_.Name;
                        return result;
                    }
                case FileInformationClass.FileIdBothDirectoryInformation:
                    {
                        FileIdBothDirectoryInformation result = new FileIdBothDirectoryInformation();
                        result.CreationTime = entry_.CreationTime;
                        result.LastAccessTime = entry_.LastAccessTime;
                        result.LastWriteTime = entry_.LastWriteTime;
                        result.ChangeTime = entry_.LastWriteTime;
                        result.EndOfFile = (long)entry_.Size;
                        result.AllocationSize = (long)GetAllocationSize(entry_.Size);
                        result.FileAttributes = GetFileAttributes(entry_);
                        result.EaSize = 0;
                        result.ShortName = GetShortName(entry_.Name);
                        result.FileId = 0;
                        result.FileName = entry_.Name;
                        return result;
                    }
                case FileInformationClass.FileIdFullDirectoryInformation:
                    {
                        FileIdFullDirectoryInformation result = new FileIdFullDirectoryInformation();
                        result.CreationTime = entry_.CreationTime;
                        result.LastAccessTime = entry_.LastAccessTime;
                        result.LastWriteTime = entry_.LastWriteTime;
                        result.ChangeTime = entry_.LastWriteTime;
                        result.EndOfFile = (long)entry_.Size;
                        result.AllocationSize = (long)GetAllocationSize(entry_.Size);
                        result.FileAttributes = GetFileAttributes(entry_);
                        result.EaSize = 0;
                        result.FileId = 0;
                        result.FileName = entry_.Name;
                        return result;
                    }
                case FileInformationClass.FileNamesInformation:
                    {
                        FileNamesInformation result = new FileNamesInformation();
                        result.FileName = entry_.Name;
                        return result;
                    }
                default:
                    {
                        throw new UnsupportedInformationLevelException();
                    }
            }
        }

        private static List<FileSystemEntry> GetFiltered(List<FileSystemEntry> entries_, string expression_)
        {
            if (expression_ == "*")
            {
                return entries_;
            }

            List<FileSystemEntry> result = new List<FileSystemEntry>();
            foreach (FileSystemEntry entry in entries_)
            {
                if (IsFileNameInExpression(entry.Name, expression_))
                {
                    result.Add(entry);
                }
            }
            return result;
        }

        private static bool IsFileNameInExpression(string fileName_, string expression_)
        {
            if (expression_ == "*")
            {
                return true;
            }
            else if (expression_.EndsWith("*")) // expression.Length > 1
            {
                string desiredFileNameStart = expression_.Substring(0, expression_.Length - 1);
                bool findExactNameWithoutExtension = false;
                if (desiredFileNameStart.EndsWith("\""))
                {
                    findExactNameWithoutExtension = true;
                    desiredFileNameStart = desiredFileNameStart.Substring(0, desiredFileNameStart.Length - 1);
                }

                if (!findExactNameWithoutExtension)
                {
                    if (fileName_.StartsWith(desiredFileNameStart, StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
                else
                {
                    if (fileName_.StartsWith(desiredFileNameStart + ".", StringComparison.OrdinalIgnoreCase) ||
                        fileName_.Equals(desiredFileNameStart, StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
            }
            else if (expression_.StartsWith("<"))
            {
                string desiredFileNameEnd = expression_.Substring(1);
                if (fileName_.EndsWith(desiredFileNameEnd, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            else if (String.Equals(fileName_, expression_, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
            return false;
        }

        private static bool ContainsWildcardCharacters(string expression_)
        {
            return (expression_.Contains("?") || expression_.Contains("*") || expression_.Contains("\"") || expression_.Contains(">") || expression_.Contains("<"));
        }

        private static ulong GetAllocationSize(ulong size_)
        {
            return (ulong)Math.Ceiling((double)size_ / ClusterSize) * ClusterSize;
        }

        private static FileAttributes GetFileAttributes(FileSystemEntry entry_)
        {
            FileAttributes attributes = 0;
            if (entry_.IsHidden)
            {
                attributes |= FileAttributes.Hidden;
            }
            if (entry_.IsReadonly)
            {
                attributes |= FileAttributes.ReadOnly;
            }
            if (entry_.IsArchived)
            {
                attributes |= FileAttributes.Archive;
            }
            if (entry_.IsDirectory)
            {
                attributes |= FileAttributes.Directory;
            }

            if (attributes == 0)
            {
                attributes = FileAttributes.Normal;
            }

            return attributes;
        }

        private static FileOptions ToFileOptions(CreateOptions createOptions_)
        {
            const FileOptions FILE_FLAG_OPEN_REPARSE_POINT = (FileOptions)0x00200000;
            const FileOptions FILE_FLAG_NO_BUFFERING = (FileOptions)0x20000000;
            FileOptions result = FileOptions.None;
            if ((createOptions_ & CreateOptions.FILE_OPEN_REPARSE_POINT) > 0)
            {
                result |= FILE_FLAG_OPEN_REPARSE_POINT;
            }
            if ((createOptions_ & CreateOptions.FILE_NO_INTERMEDIATE_BUFFERING) > 0)
            {
                result |= FILE_FLAG_NO_BUFFERING;
            }
            if ((createOptions_ & CreateOptions.FILE_RANDOM_ACCESS) > 0)
            {
                result |= FileOptions.RandomAccess;
            }
            if ((createOptions_ & CreateOptions.FILE_SEQUENTIAL_ONLY) > 0)
            {
                result |= FileOptions.SequentialScan;
            }
            if ((createOptions_ & CreateOptions.FILE_WRITE_THROUGH) > 0)
            {
                result |= FileOptions.WriteThrough;
            }
            if ((createOptions_ & CreateOptions.FILE_DELETE_ON_CLOSE) > 0)
            {
                result |= FileOptions.DeleteOnClose;
            }

            return result;
        }

        private static string ToFileOptionsString(FileOptions options_)
        {
            string result = String.Empty;
            const FileOptions FILE_FLAG_OPEN_REPARSE_POINT = (FileOptions)0x00200000;
            const FileOptions FILE_FLAG_NO_BUFFERING = (FileOptions)0x20000000;
            if ((options_ & FILE_FLAG_OPEN_REPARSE_POINT) > 0)
            {
                result += "ReparsePoint|";
                options_ &= ~FILE_FLAG_OPEN_REPARSE_POINT;
            }
            if ((options_ & FILE_FLAG_NO_BUFFERING) > 0)
            {
                result += "NoBuffering|";
                options_ &= ~FILE_FLAG_NO_BUFFERING;
            }

            if (result == String.Empty || options_ != FileOptions.None)
            {
                result += options_.ToString().Replace(", ", "|");
            }
            result = result.TrimEnd(new char[] { '|' });
            return result;
        }

        private static NTStatus ToNTStatus(Exception exception_)
        {
            if (exception_ is ArgumentException)
            {
                return NTStatus.STATUS_OBJECT_PATH_SYNTAX_BAD;
            }
            else if (exception_ is DirectoryNotFoundException)
            {
                return NTStatus.STATUS_OBJECT_PATH_NOT_FOUND;
            }
            else if (exception_ is FileNotFoundException)
            {
                return NTStatus.STATUS_OBJECT_PATH_NOT_FOUND;
            }
            else if (exception_ is IOException)
            {
                ushort errorCode = IOExceptionHelper.GetWin32ErrorCode((IOException)exception_);
                if (errorCode == (ushort)Win32Error.ERROR_SHARING_VIOLATION)
                {
                    return NTStatus.STATUS_SHARING_VIOLATION;
                }
                else if (errorCode == (ushort)Win32Error.ERROR_DISK_FULL)
                {
                    return NTStatus.STATUS_DISK_FULL;
                }
                else if (errorCode == (ushort)Win32Error.ERROR_DIR_NOT_EMPTY)
                {
                    // If a user tries to rename folder1 to folder2 when folder2 already exists, Windows 7 will offer to merge folder1 into folder2.
                    // In such case, Windows 7 will delete folder 1 and will expect STATUS_DIRECTORY_NOT_EMPTY if there are files to merge.
                    return NTStatus.STATUS_DIRECTORY_NOT_EMPTY;
                }
                else if (errorCode == (ushort)Win32Error.ERROR_ALREADY_EXISTS)
                {
                    // According to [MS-FSCC], FileRenameInformation MUST return STATUS_OBJECT_NAME_COLLISION when the specified name already exists and ReplaceIfExists is zero.
                    return NTStatus.STATUS_OBJECT_NAME_COLLISION;
                }
                else
                {
                    return NTStatus.STATUS_DATA_ERROR;
                }
            }
            else if (exception_ is UnauthorizedAccessException)
            {
                return NTStatus.STATUS_ACCESS_DENIED;
            }
            else
            {
                return NTStatus.STATUS_DATA_ERROR;
            }
        }

        private void Log(Severity severity_, string message_)
        {
            // To be thread-safe we must capture the delegate reference first
            EventHandler<LogEntry> handler = LogEntryAdded;
            if (handler != null)
            {
                handler(this, new LogEntry(DateTime.Now, severity_, "NT FileSystem Adapter", message_));
            }
        }

        private void Log(Severity severity_, string message, params object[] args_)
        {
            // To be thread-safe we must capture the delegate reference first
            EventHandler<LogEntry> handler = LogEntryAdded;
            if (handler != null)
            {
                handler(this, new LogEntry(DateTime.Now, severity_, "NT FileSystem Adapter", string.Format(message, args_)));
            }
        }

        #endregion
    }
}
