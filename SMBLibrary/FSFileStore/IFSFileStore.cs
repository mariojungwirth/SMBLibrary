using System;
using System.Collections.Generic;
using System.IO;
using Utilities;

namespace SMBLibrary.FSFileStore
{
    public interface IFSFileStore
    {
        long Size { get; }
        long FreeSpace { get; }
        string Name { get; }
        event EventHandler<LogEntry> LogEntryAdded;

        FileSystemEntry GetEntry(string path_);

        FileSystemEntry CreateDirectory(string path_);
        FileSystemEntry GetParentDirectory(string path_);
        void DeleteDirectory(string path_);
        void MoveDirectory(string from_, string to_);
        void SetAttributesDirectory(string path_, bool? isHidden_, bool? isReadonly_, bool? isArchived_);
        void SetDatesDirectory(string path_, DateTime? creationDT_, DateTime? lastWriteDT_, DateTime? lastAccessDT_);
        List<FileSystemEntry> ListEntriesInDirectory(string path_);

        FileSystemEntry CreateFile(string filename_);
        void DeleteFile(string filename_);
        void MoveFile(string from_, string to_);
        void SetAttributesFile(string path_, bool? isHidden_, bool? isReadonly_, bool? isArchived_);
        void SetDatesFile(string path_, DateTime? creationDT_, DateTime? lastWriteDT_, DateTime? lastAccessDT_);
        Stream OpenFile(string filename_, FileMode mode_, FileAccess access_, FileShare share_, FileOptions options_);
    }
}
