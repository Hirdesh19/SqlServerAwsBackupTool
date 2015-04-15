using System;

namespace SqlServerAwsBackupTool
{
    public class Program
    {
        public static int Main(string[] args)
        {
            var backupMan = new BackupManager();

            var result = backupMan.Backup(args);

            return result.ReturnCode;
        }
    }
}
