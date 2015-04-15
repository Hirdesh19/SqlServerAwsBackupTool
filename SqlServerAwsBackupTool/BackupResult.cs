using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerAwsBackupTool
{
    public class BackupResult
    {
        public int ReturnCode { get; set; }
        public string BackupName { get; set; }

        public BackupResult (int returnCode)
        {
            ReturnCode = returnCode;
        }

        public BackupResult (string backupName)
        {
            BackupName = backupName;
        }
    }
}
