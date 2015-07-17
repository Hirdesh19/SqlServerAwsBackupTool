using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerAwsBackupTool
{
    public class RestoreResult
    {
        public int ReturnCode { get; set; }
        public string[] BackupNames { get; set; }

        public RestoreResult(int returnCode)
        {
            ReturnCode = returnCode;
        }

        public RestoreResult(string[] backupNames)
        {
            BackupNames = backupNames;
        }
    }
}
