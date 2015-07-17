using Amazon.S3;
using Amazon.S3.Model;
using IniParser;
using IniParser.Model;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerAwsBackupTool
{
    public class RestoreManager
    {
        public RestoreResult Restore(string[] args)
        {
            if (args.Length < 2)
            {
                Console.Error.WriteLine("You must specify a configuration file and at least one backup file name in that order.");
                return new RestoreResult(-1);
            }

            var configFileName = args[0];

            if (!File.Exists(configFileName))
            {
                Console.Error.WriteLine("You must specify a configuration file that exists.");
                return new RestoreResult(-2);
            }

            var parser = new FileIniDataParser();
            IniData config = parser.ReadFile(configFileName);

            // SqlServer variables
            var serverName = config["sqlserver"]["server"];
            var database = config["sqlserver"]["database"];
            var tmpFolder = config["sqlserver"]["temp_dir"];

            var backupFileName = args[1];

            if (backupFileName.IndexOf("_full_") == -1)
            {
                Console.Error.WriteLine("You must specify a full backup first.");
                return new RestoreResult(-3);
            }

            var logBkpNames = new string[0];

            string[] backupFiles;

            if (args.Length > 2)
            {
                int logBkpCount = args.Length - 2;

                logBkpNames = new string[logBkpCount];

                Array.Copy(args, 2, logBkpNames, 0, logBkpCount);

                foreach (var name in logBkpNames)
                {
                    if (name.IndexOf("_full_") != -1)
                    {
                        Console.Error.WriteLine("You must specify only one full backup.");
                        return new RestoreResult(-4);
                    }
                }

                backupFiles = new string[logBkpCount + 1];

                Array.Copy(args, 1, backupFiles, 0, logBkpCount + 1);
            }
            else
            {
                backupFiles = new string[1] { backupFileName };
            }

            var backupFile = Path.Combine(tmpFolder, backupFileName);
            var awsProfile = config["aws"]["profile"];

            Amazon.Util.ProfileManager.RegisterProfile(awsProfile, config["aws"]["access_key"], config["aws"]["secret_key"]);

            var creds = Amazon.Util.ProfileManager.GetAWSCredentials(awsProfile);

            var awsClient = new AmazonS3Client(creds, Amazon.RegionEndpoint.USEast1);

            var getRequest = new GetObjectRequest()
            {
                BucketName = config["aws"]["bucket"],
                Key = backupFileName
            };

            using (var getResponse = awsClient.GetObject(getRequest))
            {
                if (!File.Exists(backupFile))
                {
                    getResponse.WriteResponseStreamToFile(backupFile);
                }
            }

            ServerConnection smoConn = new ServerConnection(config["sqlserver"]["server"]);
            Server server = new Server(smoConn);
            Restore fullDestination = new Restore();
            fullDestination.Action = RestoreActionType.Database;
            fullDestination.Database = config["sqlserver"]["database"];

            BackupDeviceItem source = new BackupDeviceItem(backupFile, DeviceType.File);
            fullDestination.Devices.Add(source);
            fullDestination.ReplaceDatabase = true;
            if (logBkpNames.Length > 0)
            {
                fullDestination.NoRecovery = true;
            }

            fullDestination.SqlRestore(server);

            if (logBkpNames.Length > 0)
            {
                for (int i = 0; i < logBkpNames.Length; i++)
                {
                    var name = logBkpNames[i];

                    Restore logDestination = new Restore();
                    logDestination.Action = RestoreActionType.Log;
                    logDestination.Database = config["sqlserver"]["database"];

                    // Set the db to no recovery on the last log restore
                    if (i == logBkpNames.Length - 1)
                    {
                        logDestination.NoRecovery = false;
                    }
                    else
                    {
                        logDestination.NoRecovery = true;
                    }

                    var localLogPath = Path.Combine(tmpFolder, name);

                    var logGetRequest = new GetObjectRequest()
                    {
                        BucketName = config["aws"]["bucket"],
                        Key = name
                    };

                    using (var getResponse = awsClient.GetObject(logGetRequest))
                    {
                        if (!File.Exists(localLogPath))
                        {
                            getResponse.WriteResponseStreamToFile(localLogPath);
                        }
                    }

                    BackupDeviceItem logSource = new BackupDeviceItem(localLogPath, DeviceType.File);
                    logDestination.Devices.Add(logSource);
                    logDestination.SqlRestore(server);
                }


            }

            smoConn.Disconnect();

            return new RestoreResult(backupFiles);
        }
    }
}
